{-# LANGUAGE OverloadedStrings, RankNTypes, TypeSynonymInstances, FlexibleInstances, OverloadedLists, DeriveGeneric  #-}

{-
    Column datastore approach
-}

module Quark.Base.Storage
    ( 
        saveColumn,
        loadColumn
        
    ) where

import Quark.Base.Column

import Data.Binary
import Data.ByteString.Lazy as BL
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as UM
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G

import Data.Text
import Data.Text.Encoding

import Data.Vector.Binary -- Binary instances for Vectors!!! 
-- So, primitive serialization for unboxed types (suitable for not very large files) should be covered automatically
-- Actually, Text is Binary so works automagically as well! Supah! :)

-- saves a column to a file
saveColumn :: (G.Vector v a, Binary (v a)) => FilePath -> v a -> IO ()
saveColumn file col = BL.writeFile file (encode col)

-- loads a column from a file
loadColumn :: Binary b => FilePath -> IO b
loadColumn file = BL.readFile file >>= return . decode




{-

import GHC.Generics (Generic)
import Control.Monad.Primitive
instance PrimMonad Get

-- | 'getMany n' get 'n' elements in order, without blowing the stack -- copy from binary package, it's hidden in there!!
getMany :: (Binary a, U.Unbox a) => Int -> Get (U.Vector a)
getMany n = go (U.fromList []) n
 where
    go vec 0 = return $! vec
    go vec i = do x <- get
                    -- we must seq x to avoid stack overflows due to laziness in
                    -- (>>=)
                  x `seq` go (U.snoc vec x) (i-1)


instance (Binary a, U.Unbox a) => Binary (U.Vector a) where
    put l  = put (U.length l) >> U.mapM_ put l
    get    = do n <- get :: Get Int
                getMany n
-}


