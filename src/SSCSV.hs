{-# LANGUAGE OverloadedStrings, TransformListComp #-}

module SSCSV
    ( 
        testRun,
        encoders,
        loadToMemory,
        processFile,
        convertFileToList,
        aggregateData,
        processAggrM,
        processAggrM2,
        aggrMapM,
        QDatabase,
        convertDB,
        convertDB'
    ) where

--      So, for the generic CSV loading we want to do the following:
--      1) Parse several lines and show them to the user so that he can mark how to parse different columns
--      1a) Maybe make a guess on types in the process
--      2) Build a list of conversion functions from ByteString to our internal representation
--      3) zipWith this list of functions with each row of the CSV file, resulting in a Vector of internal parsed representations
--      ==> Ready for further processing!


import Data.Csv
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import Data.Text
import Data.ByteString.Lazy as BL
import System.IO
import Data.Char
import Data.Time
import Text.Read
import Data.Text.Read

import Data.Time.Clock

import GHC.Exts

import Quark.Base.Data
import Quark.Base.Column

import qualified Data.HashMap as Map
import Data.Hashable

-- type CSField = QValue

qToDouble :: QValue -> Double
qToDouble (QDouble x) = x
qToDouble _ = 0.0

qToText :: QValue -> Text
qToText (QString x) = x
qToText _ = ""


csToInt :: QValue -> Int
csToInt (QInt i) = i
csToInt _ = 0

csIntToCSDouble :: QValue -> QValue
csIntToCSDouble (QInt x) = QDouble (fromIntegral x)
csIntToCSDouble (QDouble x) = QDouble x
csIntToCSDouble _ = QIllegalValue


toCSDouble :: Text -> QValue
toCSDouble s = let r = double s
                in case r of 
                    (Right (d,_) ) -> QDouble d
                    (Left _) -> QIllegalValue

toDouble :: Text -> Double
toDouble s = let r = double s
                in case r of 
                    (Right (d,_) ) -> d
                    (Left _) -> 0.0


toCSInt :: Text -> QValue
toCSInt s = let r = signed decimal s
                in case r of 
                    (Right (d,_) ) -> QInt d
                    (Left _) -> QIllegalValue

toCSString :: Text -> QValue
toCSString s = QString s

toCSDate :: Text -> QValue
toCSDate s = ws $ splitOn "." s
                where ws [x,y,z] = QDateS (csToInt $ toCSInt x, csToInt $ toCSInt y, csToInt $ toCSInt z)

myOptions = defaultDecodeOptions {
      decDelimiter = fromIntegral (ord ';')
    }


type QRecord = V.Vector QValue
type QDatabase = V.Vector QRecord

-- read csv file into memory using cassava - rethink whether we need it?
loadToMemory :: FilePath -> HasHeader -> IO (Either String (V.Vector (V.Vector Text)))
loadToMemory fp hh = decodeWith myOptions hh <$> BL.readFile fp

-- array of encoders suitable for sample*.csv files
encoders :: V.Vector (Text -> QValue)
encoders = V.fromList [toCSString, toCSInt, toCSString, toCSDate, toCSString, toCSString, toCSString, toCSString, toCSString, toCSDouble,
    toCSString, toCSString, toCSString, toCSString, toCSString, toCSString, toCSDate, toCSString] 


-- "multiply" a list of encoders by a line from a csv file, converting it according to encoders
processLine :: V.Vector (Text -> QValue) -> V.Vector Text -> QRecord
processLine enc l = V.zipWith ($) enc l

-- process the whole file: basically, chain loadToMemory with processFile and we should have what we need
processFile :: V.Vector (Text -> QValue) -> V.Vector (V.Vector Text) -> QDatabase
processFile enc f = V.map (processLine enc) f

convertFileToList :: V.Vector (V.Vector a) -> [[a]]
convertFileToList = V.toList . (V.map V.toList)

testRun fname = do 
    (Right f) <- loadToMemory fname HasHeader
    let pf = processFile encoders f
    let pfl = convertFileToList pf
    -- print pfl
    -- let output = aggregateData pfl
    let output = aggregateData pfl
    return output


sum' (x:xs) = Prelude.foldr (\acc y -> acc + y) x xs


aggregateData ls = [ [the globalRegion, the subregion, sum amount]
            | [ buckets, lifetime, opType, created, 
                subbucket, subregion, opty, 
                salesteam, product, amount, stage, 
                resID, partnerLevel, reseller, 
                globalRegion, region, closed, dealSize] <- ls
            , then group by (globalRegion, subregion) using groupWith ]

aggregateSimple ls = [ [the globalRegion, sum' amount]
            | [ buckets, lifetime, opType, created, 
                subbucket, subregion, opty, 
                salesteam, product, amount, stage, 
                resID, partnerLevel, reseller, 
                globalRegion, region, closed, dealSize] <- ls
            , then group by globalRegion using groupWith ]



-- Let's try to write this simple aggregation for vector types
aggrMap = Map.fromList [(QString "dummy", QDouble 0)]
aggrMapM = Map.fromList [( [QString "dummy",QString "dummy",QString "dummy"],  QDouble 0)]

-- convert column n to column
convertToCol n db f = V.foldr' (\ x acc -> let el = x V.! n in (f el):acc) [] db

-- for testing column algorithms
-- converts from read file
convertDB db = (V.fromList (convertToCol 14 db id) :: CText, V.fromList (convertToCol 5 db id) :: CText, U.fromList (convertToCol 9 db toDouble) :: CDouble)
-- converts from QDatabase
convertDB' db = (V.fromList (convertToCol 14 db qToText) :: CText, V.fromList (convertToCol 5 db qToText) :: CText, U.fromList (convertToCol 9 db qToDouble) :: CDouble)

processAggr n m amap line = 
    let x = line V.! n
        y = line V.! m
    in Map.insertWith (+) x y amap

processAggrM n m l k amap line = 
    let x = line V.! n
        y = line V.! m
        z = line V.! l
        w = line V.! k
    in Map.insertWith (+) [x,y,z] w amap

processAggrM2 n m k amap line = 
    let x = line V.! n
        y = line V.! m
        w = line V.! k
    in Map.insertWith (+) [x,y] w amap


-- V.foldl' (\acc x -> processAggrM 14 15 5 9 acc x) aggrMapM pf 
-- ^ in multidimension (tested with data here), foldl' is the fastest and uses less memory... 4x the aggregate!!!


processAggr1 n m fl = V.foldl (\acc x -> processAggr n m acc x) aggrMap fl  
processAggrR n m fl = V.foldr (\x acc -> processAggr n m acc x) aggrMap fl  
processAggr1' n m fl = V.foldl' (\acc x -> processAggr n m acc x) aggrMap fl  
processAggrR' n m fl = V.foldr' (\x acc -> processAggr n m acc x) aggrMap fl  -- FASTEST!!! (20% than aggregate, 20% less memory!!) - on ONE dimension


-- ****************************************************************************************************************
-- Generalized lists examples

    
-- decode hh <$> BL.readFile fp

{-


employees = [ ("Simon", "MS", "March", 80)
    , ("Erik", "MS", "March", 100)
    , ("Phil", "Ed", "May ", 40)
    , ("Gordon", "Ed", "March", 45)
    , ("Paul", "Yale", "May", 60) ]

output = [ (the dept, the month, sum salary)
    | (name, dept, month, salary) <- employees
    , then group by (dept, month) using groupWith
    , then sortWith by (sum salary)
    ]

employees1 = [ ["Simon", "MS", "80"]
    , ["Erik", "MS", "100"]
    , ["Phil", "Ed", "40"]
    , ["Gordon", "Ed", "45"]
    , ["Paul", "Yale", "60"]]

output1 = [ [the dept, Data.Text.concat salary]
    | [name, dept, salary] <- employees1
    , then group by dept using groupWith
    , then Prelude.take 5 ]




dumpAll :: FromRecord a => IO (Either String (Vector a)) -> IO ()
dumpAll cs = do
    case cs of
        Left err -> putStrLn err
        Right v -> forM_ v $ \ (name, salary :: Int) ->
            putStrLn $ name ++ " earns " ++ show salary ++ " dollars"
    
-}