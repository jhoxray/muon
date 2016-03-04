{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, DisambiguateRecordFields #-}

module FunCharts where

data SeriesType = Area | Bar | Bubble | Column | Line | Pie | Scatter
data AxisType = Linear | Log | Datetime | Category

data Axis = Axis 
    { min       :: Double
    , max       :: Double
    , axType    :: AxisType
    }

