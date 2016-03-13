{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, TypeSynonymInstances, FlexibleInstances, OverloadedLists  #-}

{-
    Column datastore approach
-}

module Quark.Base.Column
    ( 
        CInt, CDouble, CWord, CBool, CText, GenericColumn, CTable, vfold, vfold2, vfoldr2, 
        groupColumns1,
        groupColumns,
        groupColumns2,
        groupColumns3
        

    ) where

import Data.Int
import Data.Word

import qualified Data.Vector.Unboxed as U
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G

import Data.Text
import Data.Time
import Data.Time.Calendar
-- import Text.Read
import Data.Text.Read

import GHC.Exts

-- import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as Map
import Data.Hashable


type Map = Map.HashMap

-- primitive datatypes columns
type CInt = U.Vector Int64
type CDouble = U.Vector Double
type CWord = U.Vector Word64
type CBool = U.Vector Bool

-- boxed column for strings
type CText = V.Vector Text

-- is there a better way to do this with RankN? Trying to put all columns regardless of type to a Map
data GenericColumn = CInt CInt | CDouble CDouble | CWord CWord | CBool CBool | CText CText deriving (Show)

-- Map from column names to columns, representing a table
type CTable = Map Text GenericColumn 


-- group by 1 column, with only 1 aggregation function f to be used on column yys
groupColumns xxs f yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumns #-}    
-- group by 2 columns, with only 1 aggregation function
groupColumns2 (x,y) f ws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (y G.! i)) (ws G.! i) acc) (Map.fromList []) x 
{-# INLINE groupColumns2 #-}
-- group by 3 columns, with only 1 aggregation function -- etc...
groupColumns3 (x,y,z) f ws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (y G.! i), (z G.! i)) (ws G.! i) acc) (Map.fromList []) x 
{-# INLINE groupColumns3 #-}


{- right fold performs worse
groupColumns3R (x,y,z) f ws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (y G.! i), (z G.! i)) (ws G.! i) acc) (Map.fromList []) x 
-}


-- group by (x:xs) - list of columns to groupby, with only 1 aggregation function f to be used on column ws
-- this one is VERY SLOW - because of PRelude??
groupColumns1 f (x:xs) ws = 
    G.ifoldl' pline (Map.fromList []) x
    where 
        pline acc i l = Map.insertWith f (l: (Prelude.map (G.! i) xs)) (ws G.! i) acc
        


vfold f z xxs yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) z xxs 

vfold2 f z xxs yys wws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 
vfoldr2 f z xxs yys wws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 


-- encoded text column for faster processing (encodedCol is a column with ints that correspond to position of string in the values vector)
data ConvertedTextColumn = ConvertedTextColumn { encodedCol :: CInt, values :: CText}

-- aggregate :: CTable -> Text -> 

-- ***************************************************************************************************************************************
-- TEST DATA
-- ***************************************************************************************************************************************

cint :: CInt
cint = U.fromList [1,2,43,234,23,412,24,12,4,252,1,2,43,234,23,412,24,12,4,252]

cdouble :: CDouble
cdouble = U.fromList [13,23,433,23.4,233,4.12,22,1.2,4,-25.2, 1,2,43,234,23,412,24,12,4,252]

creg :: CText
creg = V.fromList ["EMEA", "NA", "EMEA", "RoW", "NA", "RoW", "EMEA", "EMEA", "NA", "RoW", "EMEA", "NA", "RoW", "NA", "RoW", "NA", "RoW", "EMEA", "NA", "EMEA"]

ctable :: CTable
ctable = Map.fromList [("# ops", CInt cint), ("$ rev", CDouble cdouble), ("region", CText creg)]


{-
pfold :: (Ord t, Hashable t, U.Unbox t1) =>
  (t1 -> t1 -> t1) -> Map.Map t t1 -> V.Vector t -> U.Vector t1 -> Map.Map t t1

pfold f z [] [] = z
pfold f z xxs yys = Map.insertWith f x y (pfold f z xs ys)
                     where  x = G.head xxs
                            xs = G.tail xxs
                            y = G.head yys
                            ys = G.tail yys


-}









