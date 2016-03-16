{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, 
                TypeSynonymInstances, FlexibleInstances, OverloadedLists, DeriveGeneric  #-}

{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts,
             TypeFamilies, ScopedTypeVariables #-}

{-
    Column datastore approach
-}

module Quark.Base.Column
    ( 
        CInt, CDouble, CWord, CBool, CText, 
        GenericColumn (..), 
        CTable,
        unpackCInt,
        unpackCDouble,
        unpackCWord,
        unpackCBool,
        unpackCText
        
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
import Data.Binary

import GHC.Exts

-- import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as Map
import Data.Hashable

import Data.Vector.Binary -- Binary instances for Vectors!!! 
-- So, primitive serialization for unboxed types (suitable for not very large files) should be covered automatically
-- Actually, Text is Binary so works automagically as well! Supah! :)

-- import GHC.Generics (Generic)

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

-- encoded text column for faster processing (encodedCol is a column with ints that correspond to position of string in the values vector)
data ConvertedTextColumn = ConvertedTextColumn { encodedCol :: CInt, values :: CText}

-- Binary instance for Generic Column
instance Binary GenericColumn where
    put (CInt v) =      do put (0 :: Word8) >> put v
    put (CDouble v) =   do put (1 :: Word8) >> put v
    put (CWord v) =     do put (2 :: Word8) >> put v
    put (CBool v) =     do put (3 :: Word8) >> put v
    put (CText v) =     do put (4 :: Word8) >> put v

    get = do t <- get :: Get Word8
             case t of
                0 -> get >>= return . CInt 
                1 -> get >>= return . CDouble
                2 -> get >>= return . CWord
                3 -> get >>= return . CBool
                4 -> get >>= return . CText


-- Map from column names to columns, representing a table
type CTable = Map Text GenericColumn 


-- functions extracting Vectors from GenericColumn
unpackCInt (CInt v) = v
unpackCInt _ = U.fromList []
unpackCDouble (CDouble v) = v
unpackCDouble _ = U.fromList []
unpackCWord (CWord v) = v
unpackCWord _ = U.fromList []
unpackCBool (CBool v) = v
unpackCBool _ = U.fromList []
unpackCText (CText v) = v
unpackCText _ = V.fromList []

{-
unpackVector :: G.Vector v a => GenericColumn -> v a
unpackVector (CInt v) = v
unpackVector (CDouble v) = v
unpackVector (CWord v) = v
-- unpackVector (CBool v) = v
unpackVector _ = U.fromList [] 

-- instance Binary GenericColumn where
-}  



-- ***************************************************************************************************************************************
-- TEST DATA
-- ***************************************************************************************************************************************

cint :: CInt
cint = U.fromList [1,2,43,234,23,412,24,12,4,252,1,2,43,234,23,412,24,12,4,252]

cdouble :: CDouble
cdouble = U.fromList [13,23,433,23.4,233,4.12,22,1.2,4,-25.2, 1,2,43,234,23,412,24,12,4,252]

creg :: CText
creg = V.fromList ["EMEA", "NA", "EMEA", "RoW", "NA", "RoW", "EMEA", "EMEA", "NA", "RoW", "EMEA", "NA", "RoW", "NA", "RoW", "NA", "RoW", "EMEA", "NA", "EMEA"]

-- ctable :: CTable
-- ctable = Map.fromList [("# ops", CInt cint), ("$ rev", CDouble cdouble), ("region", CText creg)]


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









