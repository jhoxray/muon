{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, TypeSynonymInstances, FlexibleInstances, OverloadedLists  #-}

{-
    Column datastore approach
-}

module Quark.Base.Column
    ( 
        CInt, CDouble, CWord, CBool, CText, GenericColumn, CTable, pfold, vfold, vfold2
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

import qualified Data.HashMap as Map
import Data.Hashable

-- primitive datatypes columns
type CInt = U.Vector Int64
type CDouble = U.Vector Double
type CWord = U.Vector Word64
type CBool = U.Vector Bool

-- boxed column for strings
type CText = V.Vector Text

class Column a where 
    checkType :: a -> Text

instance Column CInt where
    checkType _ = "Int"

instance Column CDouble where
    checkType _ = "Double"

instance Column CWord where
    checkType _ = "Word"

instance Column CBool where
    checkType _ = "Bool"

instance Column CText where
    checkType _ = "Text"


-- is there a better way to do this with RankN? Trying to put all columns regardless of type to a Map
data GenericColumn = CInt CInt | CDouble CDouble | CWord CWord | CBool CBool | CText CText deriving (Show)

-- Map from column names to columns, representing a table
type CTable = Map.Map Text GenericColumn 

-- grouping by groupCol, aggregating by aggrCol with function func
-- aggregateSimple groupCol aggrCol func
-- aggregateSimpleLine x y f amap = Map.insertWith f x y amap

-- aggregateSimle (x:xs) (y:ys) f amap = aggregateSimpleLine x y f amap

pfold :: (Ord t, Hashable t, U.Unbox t1) =>
  (t1 -> t1 -> t1) -> Map.Map t t1 -> V.Vector t -> U.Vector t1 -> Map.Map t t1

pfold f z [] [] = z
pfold f z xxs yys = Map.insertWith f x y (pfold f z xs ys)
                     where  x = G.head xxs
                            xs = G.tail xxs
                            y = G.head yys
                            ys = G.tail yys


vfold f z xxs yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) z xxs 

vfold2 f z xxs yys wws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 

-- plfold f z [] [] = z
-- plfold f z (x:xs) (y:ys) = Map.insertWith f x y (pfold f z xs ys)


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












