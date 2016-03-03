{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving #-}

module Highcharts where

import Haste
-- import Control.Applicative
-- import Haste.JSON
import Haste.Foreign
import Haste.DOM
import Haste.Prim
import Haste.Events

import FunCharts

newtype Chart = Chart JSAny deriving (ToAny, FromAny)

data ChartOptions = ChartOptions
    { renderTo :: JSString
    , chartType :: JSString
    } 

instance ToAny ChartOptions where
    toAny co = 
        let c = toObject [("renderTo", toAny $ renderTo co), ("type", toAny $ chartType co)] 
        in toObject[("chart", c)]

defaultChartOptions = ChartOptions {renderTo = "chart1", chartType = "line"}


newChart :: ChartOptions -> IO Chart
newChart co = ffi "(function (opt) { var c1 = new Highcharts.Chart(opt); return c1; })" (toAny co)