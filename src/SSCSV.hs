{-# LANGUAGE OverloadedStrings, TransformListComp #-}

module SSCSV
    ( 
    ) where

--      So, for the generic CSV loading we want to do the following:
--      1) Parse several lines and show them to the user so that he can mark how to parse different columns
--      1a) Maybe make a guess on types in the process
--      2) Build a list of conversion functions from ByteString to our internal representation
--      3) zipWith this list of functions with each row of the CSV file, resulting in a Vector of internal parsed representations
--      ==> Ready for further processing!


import Data.Csv
import qualified Data.Vector as V
import Data.Text
import Data.ByteString.Lazy as BL
import System.IO
import Data.Char
import Data.Time
import Text.Read
import Data.Text.Read

import GHC.Exts

newtype DefaultToZero = DefaultToZero Int deriving (Show)

data CSField = Double Double | Int Int | CSString Text | Date (Int, Int, Int) | NaN deriving (Show, Eq, Ord)

plusCSField :: CSField -> CSField -> CSField
plusCSField (Double x) (Double y) = Double (x + y)
plusCSField (Int x) (Int y) = Int (x + y)
plusCSField _ _ = NaN

minusCSField :: CSField -> CSField -> CSField
minusCSField (Double x) (Double y) = Double (x - y)
minusCSField (Int x) (Int y) = Int (x - y)
minusCSField _ _ = NaN

timesCSField :: CSField -> CSField -> CSField
timesCSField (Double x) (Double y) = Double (x * y)
timesCSField (Int x) (Int y) = Int (x * y)
timesCSField _ _ = NaN

negateCSField :: CSField -> CSField
negateCSField (Double x) = Double (negate x)
negateCSField (Int x) = Int (negate x)
negateCSField x = x

absCSField :: CSField -> CSField
absCSField (Double x) = Double (abs x)
absCSField (Int x) = Int (abs x)
absCSField x = x

signumCSField :: CSField -> CSField
signumCSField (Double x) = Double (signum x)
signumCSField (Int x) = Int (signum x)
signumCSField x = x


instance  Num CSField  where
    (+) = plusCSField
    (-) = minusCSField
    (*) = timesCSField
    negate         = negateCSField
    fromInteger x  =  Int (fromInteger x)

    abs = absCSField
    signum = signumCSField

csToInt :: CSField -> Int
csToInt (Int i) = i
csToInt _ = 0

csIntToCSDouble :: CSField -> CSField
csIntToCSDouble (Int x) = Double (fromIntegral x)
csIntToCSDouble (Double x) = Double x
csIntToCSDouble _ = NaN


toCSDouble :: Text -> CSField
toCSDouble s = let r = double s
                in case r of 
                    (Right (d,_) ) -> Double d
                    (Left _) -> NaN

toCSInt :: Text -> CSField
toCSInt s = let r = signed decimal s
                in case r of 
                    (Right (d,_) ) -> Int d
                    (Left _) -> NaN

toCSString :: Text -> CSField
toCSString s = CSString s

toCSDate :: Text -> CSField
toCSDate s = ws $ splitOn "." s
                where ws [x,y,z] = Date (csToInt $ toCSInt x, csToInt $ toCSInt y, csToInt $ toCSInt z)

myOptions = defaultDecodeOptions {
      decDelimiter = fromIntegral (ord ';')
    }

-- read csv file into memory using cassava - rethink whether we need it?
loadToMemory :: FilePath -> HasHeader -> IO (Either String (V.Vector (V.Vector Text)))
loadToMemory fp hh = decodeWith myOptions hh <$> BL.readFile fp

-- array of encoders suitable for sample*.csv files
encoders :: V.Vector (Text -> CSField)
encoders = V.fromList [toCSString, toCSInt, toCSString, toCSDate, toCSString, toCSString, toCSString, toCSString, toCSString, toCSDouble,
    toCSString, toCSString, toCSString, toCSString, toCSString, toCSString, toCSDate, toCSString] 


-- "multiply" a list of encoders by a line from a csv file, converting it according to encoders
processLine :: V.Vector (Text -> CSField) -> V.Vector Text -> V.Vector CSField
processLine enc l = V.zipWith ($) enc l

-- process the whole file: basically, chain loadToMemory with processFile and we should have what we need
processFile :: V.Vector (Text -> CSField) -> V.Vector (V.Vector Text) -> V.Vector (V.Vector CSField)
processFile enc f = V.map (processLine enc) f

convertFileToList :: V.Vector (V.Vector a) -> [[a]]
convertFileToList = V.toList . (V.map V.toList)

testRun fname = do 
    (Right f) <- loadToMemory fname HasHeader
    let pf = processFile encoders f
    let pfl = convertFileToList pf
    -- print pfl
    let output = [ [the globalRegion, the region, sum' amount]
            | [ buckets, lifetime, opType, created, 
                subbucket, subregion, opty, 
                salesteam, product, amount, stage, 
                resID, partnerLevel, reseller, 
                globalRegion, region, closed, dealSize] <- pfl
            , then group by (globalRegion, region) using groupWith ]
    return output


sum' (x:xs) = Prelude.foldr (\acc y -> acc + y) x xs

aggregateData ls = [ [the globalRegion, sum' amount]
            | [ buckets, lifetime, opType, created, 
                subbucket, subregion, opty, 
                salesteam, product, amount, stage, 
                resID, partnerLevel, reseller, 
                globalRegion, region, closed, dealSize] <- ls
            , then group by globalRegion using groupWith ]


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