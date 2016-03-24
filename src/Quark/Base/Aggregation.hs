{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, 
                TypeSynonymInstances, FlexibleInstances, OverloadedLists, 
                DeriveGeneric, BangPatterns, FlexibleContexts, TypeFamilies, 
                GADTs, MultiParamTypeClasses, InstanceSigs  #-}

{-
    Aggregation functions
-}

module Quark.Base.Aggregation
    ( 
        groupColumnsGen,
        groupColumns1,
        groupColumns2,
        groupColumns3,
        groupColumnsST,
        groupColumnsH,
        groupColumnsFList,
        groupColumnsG1A1,
        groupColumnsG2A1,
        groupColumnsG3A1,
        groupColumnsG1A2,
        groupColumnsG1A3,
        groupColumnsG2A2,
        groupColumnsG3A2,
        groupColumnsG2A3,
        groupColumnsG3A3,

        sumGV,
        countGV,
        aggG,
        listGV
        

    ) where

import Quark.Base.Column
import Quark.Base.Raw
import Quark.Base.Data

import qualified Data.HashMap.Strict as Map
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector as V

import Data.Text

import Data.Hashable
import Data.Int

import Control.Monad.ST
import Data.STRef

import System.IO

import Data.Tuple.Select (sel1)

import qualified Data.List as L

-- import qualified Data.HashTable.ST.Cuckoo as C
-- import qualified Data.HashTable.Class as H
-- type HashTable s k v = C.HashTable s k v

import qualified Data.HashTable.IO as H
type HashTable k v = H.LinearHashTable k v





-- groupAggregate (x:[]) = groupColumns x
-- groupAggregate (x1:x2:[]) = groupColumns2 (x1,x2) 

-- ok, looks like doing 2 passes one after another (or in parallel using threads even better!) is actually faster than
-- aggregating by 2 functions in parallel in tuples in one pass.
-- So, we just rename functions that aggregate by 1 col only and then will figure out how to combine them properly
-- ALTHOUGH: we may want to pass THE SAME map around so that it gets filled in sequentially (won't work in parallel obviously)

-- with explicit map:
groupColumns1 xxs f g ys m = G.ifoldl' (\ !acc i x -> Map.insertWith f x (g $ ys G.! i) acc) m xxs 
{-# INLINE groupColumns1 #-}

groupColumns2 xt f g ys m = G.ifoldl' (\ !acc i x -> Map.insertWith f (mkGTuple2 xt i) (g $ ys G.! i) acc) m (fst xt) 
{-# INLINE groupColumns2 #-}  

groupColumns3 xt f g ys m = G.ifoldl' (\ !acc i x -> Map.insertWith f (mkGTuple3 xt i) (g $ ys G.! i) acc) m (sel1 xt) 
{-# INLINE groupColumns3 #-}  


groupColumnsG1A1 ::
  (Eq k, G.Vector v k, G.Vector v2 r, Hashable k) =>
  v k -> (v1 -> v1 -> v1) -> (r -> v1) -> v2 r -> Map.HashMap k v1
groupColumnsG1A1 xxs f g ys = G.ifoldl' (\ !acc i x -> Map.insertWith f x (g $ ys G.! i) acc) Map.empty xxs 
{-# INLINE groupColumnsG1A1 #-}

groupColumnsG2A1 xt f g ys = G.ifoldl' (\ !acc i x -> Map.insertWith f (mkGTuple2 xt i) (g $ ys G.! i) acc) Map.empty (fst xt) 
{-# INLINE groupColumnsG2A1 #-}  

groupColumnsG3A1 xt f g ys = G.ifoldl' (\ !acc i x -> Map.insertWith f (mkGTuple3 xt i) (g $ ys G.! i) acc) Map.empty (sel1 xt) 
{-# INLINE groupColumnsG3A1 #-}  

-- pattern GnAm - group by n cols aggregate by m cols
-- what to do about boilerplate?????? --> see zipFTuples and mkGTuple helper functions
{-
groupColumnsG1A2 xs f g ys f1 g1 ys1 = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 (f,f1)) x (mkGTuple2 (g, g1) (ys, ys1) i) acc) Map.empty xs 
-}        
-- changing signature to use tuples: ft = (f1, f2), gt = (g1, g2), yt = (y1, y2), xt = (x1, x2, ...) etc
-- this set of functions takes set of tuples: 
--      xt - columns to group by
--      yt - columns to aggregate by
--      gt - functions applied to each member of yt before applying ft
--      ft - aggregation functions
-- So, groupby col1, col2 aggregate sum col3, count col4 will be transformed to:
-- groupColumnsG2A2 (col1, col2) ((+), (+)) (id, const 1) (col3, col4)
-- (as sum == (+) id while count == (+) (const 1))

groupColumnsG1A2 xs ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) x (mkATuple2 gt yt i) acc) Map.empty xs 
{-# INLINE groupColumnsG1A2 #-}  

groupColumnsG1A3 xs ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) x (mkATuple3 gt yt i) acc) Map.empty xs 
{-# INLINE groupColumnsG1A3 #-}  

groupColumnsG2A2 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) (mkGTuple2 xt i) (mkATuple2 gt yt i) acc) Map.empty (fst xt) 
{-# INLINE groupColumnsG2A2 #-}  

groupColumnsG2A3 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) (mkGTuple2 xt i) (mkATuple3 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG2A3 #-}  

groupColumnsG3A2 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) (mkGTuple3 xt i) (mkATuple2 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG3A2 #-}  

groupColumnsG3A3 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) (mkGTuple3 xt i) (mkATuple3 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG3A3 #-}  

{-# INLINE zipFTuples2 #-}  
zipFTuples2 (f1, f2) (x1,x2) (y1,y2) = (f1 x1 y1, f2 x2 y2)
{-# INLINE zipFTuples3 #-}  
zipFTuples3 (f1, f2, f3) (x1,x2, x3) (y1,y2, y3)= (f1 x1 y1, f2 x2 y2, f3 x3 y3)

{-# INLINE mkATuple2 #-}  
mkATuple2 (g1, g2) (y1, y2) i = (g1 $ y1 G.! i, g2 $ y2 G.! i)
{-# INLINE mkATuple3 #-}  
mkATuple3 (g1, g2, g3) (y1, y2, y3) i = (g1 $ y1 G.! i, g2 $ y2 G.! i, g3 $ y3 G.! i)

{-# INLINE mkGTuple2 #-}  
mkGTuple2 (x1, x2) i = (x1 G.! i, x2 G.! i)
{-# INLINE mkGTuple3 #-}  
mkGTuple3 (x1, x2, x3) i = (x1 G.! i, x2 G.! i, x3 G.! i)


{- **************************************************************************************************** -}

-- works with all num functions
aggG :: GenericColumn -> (forall b. (Show b, Num b) => b->b->b) -> GenericColumn -> VPair
aggG (CText xs) f (CInt ys) = MkHM $ groupColumnsG1A1 xs f id ys
aggG (CText xs) f (CDouble ys) = MkHM $ groupColumnsG1A1 xs f id ys

aggTD :: V.Vector Text -> (b->b->b) -> (Double->b) -> U.Vector Double -> Map.HashMap Text b
aggTD = groupColumnsG1A1 

sumGV :: GenericColumn -> GenericColumn -> VPair
sumGV (CText xs) (CInt ys) = MkHM $ groupColumnsG1A1 xs (+) id ys
sumGV (CText xs) (CDouble ys) = MkHM $ groupColumnsG1A1 xs (+) id ys

listGV :: GenericColumn -> GenericColumn -> VPair
listGV (CText xs) (CInt ys) = MkHM $ groupColumnsG1A1 xs (++) (:[]) ys
listGV (CText xs) (CDouble ys) = MkHM $ groupColumnsG1A1 xs (++) (:[]) ys
listGV (CText xs) (CText ys) = MkHM $ groupColumnsG1A1 xs (++) (:[]) ys


countGV :: GenericColumn -> GenericColumn -> GenHash
countGV (CText xs) (CInt ys) = GHInt $
        G.ifoldl' (\ !acc i x -> Map.insertWith (+) x 1 acc) Map.empty xs
countGV (CText xs) (CDouble ys) = GHInt $
        G.ifoldl' (\ !acc i x -> Map.insertWith (+) x 1 acc) Map.empty xs
countGV (CText xs) (CText ys) = GHInt $
        G.ifoldl' (\ !acc i x -> Map.insertWith (+) x 1 acc) Map.empty xs


data GenHash = GHInt (Map.HashMap Text Int64) | GHDouble (Map.HashMap Text Double) deriving (Show)

{-
instance AggVG Double where
    -- aggGV :: GenericColumn -> (b->b->b) -> (Double->b) -> GenericColumn -> Map.HashMap Text b
    aggGV (CText x) f g (CDouble y) = aggTD x f g y
    sumGV (CText x) (CDouble y) = groupColumnsG1A1 x sum id y 
                               where sum a b = a + b
-}  
    

{- **************************************************************************************************** -}

{-# INLINE groupColumnsFList #-}
-- works, but has obvious limitations (aggregating columns need to be of the same type)
groupColumnsFList ::
  (Eq k, G.Vector v a1, G.Vector v1 k, Hashable k) =>
  v1 k -> [a -> a -> a] -> [a1 -> a] -> [v a1] -> Map.HashMap k [a]
groupColumnsFList xxs fs gs yys = 
    let pline !acc i x = Map.insertWith func x (L.zipWith ($) gs (L.map (G.! i) yys)) acc
        func = L.zipWith3 ($) fs
    in G.ifoldl' pline Map.empty xxs




groupColumnsST xxs f yys = ifoldlST (\ !acc i x -> Map.insertWith f x (yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumnsST #-}    

{-# INLINE groupColumnsH #-}    
groupColumnsH :: (Eq k, G.Vector v k, G.Vector v1 v2, Hashable k) =>
    v k -> (v2 -> v2 -> v2) -> v1 v2 -> IO (HashTable k v2)
groupColumnsH xs f ys = do 
    ht <- H.new 
    flip G.imapM_ xs $ \i x -> do  
        let !y = ys G.! i
        acc <- H.lookup ht x
        case acc of 
            Just v -> H.insert ht x (f y v)
            Nothing -> H.insert ht x y
    return ht


{-
groupColumns' :: GenericVector -> (forall a. Num a => a->a->a) -> NumericVector -> IO()
groupColumns' (GenericVector v) f (NumericVector w) = 
    do let res = groupColumns v f w
       putStrLn $ show res

-}
{- right fold performs worse
groupColumns3R (x,y,z) f ws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (y G.! i), (z G.! i)) (ws G.! i) acc) (Map.fromList []) x 
-}


-- group by (x:xs) - list of columns to groupby, with only 1 aggregation function f to be used on column ws
-- this one is VERY SLOW - because of PRelude??
groupColumnsGen f (x:xs) ws = 
    G.ifoldl' pline (Map.fromList []) x
    where 
        pline acc i l = Map.insertWith f (l: (Prelude.map (G.! i) xs)) (ws G.! i) acc
{-# INLINE groupColumnsGen #-}     


vfold f z xxs yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) z xxs 

vfold2 f z xxs yys wws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 
vfoldr2 f z xxs yys wws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 


-- foldlST :: (a -> b -> a) -> a -> [b] -> a
foldlST f acc xs = runST $ do
    acc' <- newSTRef acc            -- Create a variable for the accumulator
 
    G.forM_ xs $ \x -> do             -- For each x in xs...
 
        a <- readSTRef acc'         -- read the accumulator
        writeSTRef acc' (f a x)     -- apply f to the accumulator and x
 
    readSTRef acc'                  -- and finally read the result


{-# INLINE ifoldlST #-}
ifoldlST f acc xs = runST $ do
    acc' <- newSTRef acc            -- Create a variable for the accumulator
 
    flip G.imapM_ xs $ \i x -> do             -- For each x in xs...
 
        a <- readSTRef acc'         -- read the accumulator
        writeSTRef acc' (f a i x)     -- apply f to the accumulator and x
 
    readSTRef acc'                  -- and finally read the result
