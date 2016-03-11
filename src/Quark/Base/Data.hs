{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes #-}

{-
    Base data definition for further data manipulation
-}

module Quark.Base.Data
    ( 
    ) where

import qualified Data.Vector as V
import Data.Text
import Data.Time
import Data.Time.Calendar
-- import Text.Read
import Data.Text.Read

import GHC.Exts

-- primitive datatypes
data QValue = QString Text
            | QDouble !Double
            | QInt !Int
            | QDate !Day -- # of days from 1858-11-17
            | QDateTime !Int -- # of milliseconds as in Unix time
            | QBool !Bool
            | QMoney !Int -- # representing currency in cents, i.e. 130.23 USD will be 13023 Money - for FAST processing
            | QNull
            | QIllegalValue
              deriving (Eq, Ord, Show)

type KVP = V.Vector (Text, QValue)

-- ok, using RankNTypes here - need to be able to inject binary operation that acts on ANY Num types.
-- That is what the (forall a. Num a =>  a -> a -> a) signature defines - polymorfic function good for ALL Num types.
-- Otherwise it wouldn't work on both Ints and Doubles
injectBinOp :: (forall a. Num a =>  a -> a -> a) -> QValue -> QValue -> QValue
injectBinOp binOp (QInt x) (QInt y) = QInt (binOp x y)
injectBinOp binOp (QDouble x) (QDouble y) = QDouble (binOp x y)
injectBinOp binOp (QDouble x) (QInt y) = QDouble (binOp x  (fromIntegral y) )
injectBinOp binOp (QInt x) (QDouble y) = QDouble (binOp (fromIntegral x) y )
injectBinOp binOp (QMoney x) (QMoney y) = QMoney (binOp x y)
-- injectBinOp binOp (QDate x) (QDate y) = QDate (binOp x y) -- apparently, Day is not part of Num
injectBinOp binOp (QDateTime x) (QDateTime y) = QDateTime (binOp x y)
injectBinOp binOp _ _ = QIllegalValue

injectOp :: (forall a. Num a => a -> a ) -> QValue -> QValue
injectOp op (QInt x) = QInt (op x)
injectOp op (QDouble x) = QDouble (op x)

instance  Num QValue where
    (+) = injectBinOp (+)
    (-) = injectBinOp (-)
    (*) = injectBinOp (*)
    negate         = injectOp negate
    fromInteger x  =  QInt (fromInteger x)

    abs = injectOp abs
    signum = injectOp signum

instance Enum QValue where
    toEnum = QInt 
    fromEnum (QInt i) = i

{-


rankN :: (forall n. Num n => n -> n) -> (Int, Double)
rankN f = (f 1, f 1.0)

-}

-- ******************************************************************************************************************************************
-- Smart in memory data structures
-- ******************************************************************************************************************************************
{-
    We are going to start with converting csv-like table to the collection of Maps and optimized (int-based) table.
    Maps will be used for filtering (as they only have unique values stored), table - for map/reduce
    In the table - we will change all strings to ints 
-}













