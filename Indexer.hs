{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Resource
import           Control.Retry
import           Data.Aeson
import           Data.Aeson.Encode.Pretty
import           Data.Attoparsec.ByteString.Char8 (Parser, decimal, endOfInput,
                                                   parseOnly, skipSpace,
                                                   takeByteString)
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Char8            as BSC
import           Data.Conduit                     (awaitForever, (=$=))
import qualified Data.Conduit                     as C
import           Data.Conduit.Async               (buffer)
import           Data.Conduit.Combinators         (conduitVector,
                                                   linesUnboundedAscii,
                                                   sourceDirectory, sourceFile)
import qualified Data.Conduit.Combinators         as C
import           Data.Digest.Pure.SHA
import           Data.Fixed
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import           Data.Time
import           Data.Time.ISO8601
import qualified Data.Vector                      as V
import           Database.Bloodhound
import qualified Filesystem.Path.CurrentOS        as P
import qualified Network.HTTP.Client              as HTTP

type BS = BS.ByteString

main :: IO ()
main = runResourceT (buffer bufferSize producer consumer)
    where
    producer =
            sourceDirectory archive
        =$= awaitForever fileLines
        =$= C.concatMap (either (const []) (: []) . uncurry (makeDoc tzOffset))
    consumer =
            C.map (uncurry (BulkCreate index docType))
        =$= (conduitVector bulkSize :: C.Conduit a (ResourceT IO) (V.Vector a))
        =$= C.mapM_ (lift . void . httpWithRetry . bulk server . V.toList)
    archive    = "archive"
    index      = IndexName   "freenode-haskell2"
    docType    = MappingName "message"
    server     = Server      "http://localhost:9200"
    tzOffset   = negate 7
    bufferSize = 100000
    bulkSize   = 1000

fileLines :: P.FilePath -> C.Producer (ResourceT IO) (BS, BS)
fileLines file = sourceFile file
    =$= linesUnboundedAscii
    =$= C.map ((BSC.pack . P.encodeString . P.filename) file, )

makeDoc :: Int -> BS -> BS -> Either String (DocId, Value)
makeDoc tzOffset dateString line = do
    day             <- parseOnly (dateYYMMDD <* endOfInput) dateString
    (time, content) <- parseOnly ((,) <$> timeHHMMSS <* skipSpace <*> takeByteString) line
    let dateTime = localTimeToUTC (hoursToTimeZone tzOffset) (LocalTime day time)
    let doc = object [ ("sent", (String . T.pack . formatISO8601) dateTime)
                     , ("content", (String . T.decodeLatin1) content)
                     ]
    return (generateDocId doc, doc)
    where
    generateDocId = DocId . showDigest . sha1 . encodePretty' deterministic
    deterministic = Config { confIndent = 0, confCompare = compare }

dateYYMMDD :: Parser Day
dateYYMMDD = fromGregorian
    <$> ((2000 +) <$> decimal)
    <* "." <*> decimal
    <* "." <*> decimal

timeHHMMSS :: Parser TimeOfDay
timeHHMMSS = TimeOfDay
    <$> decimal
    <* ":" <*> decimal
    <* ":" <*> (MkFixed . (10 ^ (12 :: Integer) *) <$> decimal)

httpWithRetry :: IO a -> IO a
httpWithRetry = recovering policy [handler]
    where
    policy    = exponentialBackoff (10 ^ (6 :: Integer))
    handler _ = Handler $ \(_ :: HTTP.HttpException) -> return True
