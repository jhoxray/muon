{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, DisambiguateRecordFields #-}

module FunCharts where

data SeriesType = Area | Bar | Bubble | Column | Line | Pie | Scatter
data AxisType = Linear | Log | Datetime | Category

data Axis = Axis 
    { min       :: Double
    , max       :: Double
    , axType    :: AxisType
    , f         :: Int -> Int
    }

data ViewAxis = ViewAxis 
    { axis      :: Axis
    , g         :: Int -> Double -> Maybe String
    }

testAxis = Axis {min = 10.0, max = 30.0, axType = Linear, f = \x -> x * 2}