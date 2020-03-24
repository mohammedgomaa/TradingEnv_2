# -*- coding: utf-8 -*-

import gym
import json
import math
import numpy as np 
from gym.utils import seeding
from gym import error, spaces, utils
from sklearn.preprocessing import MinMaxScaler
#from sklearn.metrics import mean_squared_error
from concurrent import futures


class TradingEnv_Data():
        def __init__(self, DataPath ,SourceType ,DataType):
            '''
            DataPath   : data location on the HDD
            SourceType : the format of the data
                          * parquit
                          * CSV
                          * Our costum joson format   'CJF'
            DataType   : the type of the market data
                          * OCHL_Data
                          * Market_Snapshot {tick , orderbook , markethistory }
                          * OrderBook
            TODO: provide engineerid featurs , technical indicators , and sentment
            '''

            self.DataPath   = DataPath
            self.SourceType = SourceType
            self.DataType   = DataType
            self._ReadData()

        def _ReadData(self):

            if self.SourceType == 'parquit' :



            '''

                if self.DataType == 'OCHL_Data' :
                    # read  OCHL
                elif self.DataType == 'Market_Snapshot':
                    # read  Market_Snapshot
                elif self.DataType == 'OrderBook'
                    # read  OrderBook
                else:
                    print(' Not Supported Data Type ')
            ب'''

            elif self.SourceType == 'CSV':




            '''

                if self.DataType == 'OCHL_Data' :
                    # read  OCHL
                elif self.DataType == 'Market_Snapshot':
                    # read  Market_Snapshot
                elif self.DataType == 'OrderBook'
                    # read  OrderBook
                else:
                    print(' Not Supported Data Type ')
                    
            '''

            elif self.SourceType == 'CJF' :



            '''

                if self.DataType == 'OCHL_Data' :
                    # read  OCHL
                elif self.DataType == 'Market_Snapshot':
                    # read  Market_Snapshot
                    self._Read_CJF()
                elif self.DataType == 'OrderBook'
                    # read  OrderBook
                    self._Read_CJF() # edit to CJF to selct betwen
                else:
                    print(' Not Supported Data Type ')

            else:
                print(' Not Supported Source Type ')
            '''

        def _Read_Parquit(self):
            # read parquit data


        def _Read_CSV(self):
            # read CSV

        def _Read_CJF(self):

             with open(self.DataPath) as file:
                 for line in file:
                     json_data.append(json.loads(line))

             global json_data
             # convert an array of values into a dataset matrix
             Tick_Ask = []
             Tick_Bid = []
             Tick_Last = []
             OrderBook_buy_Rate = []
             OrderBook_buy_Quantity = []
             OrderBook_sell_Quantity = []
             OrderBook_sell_Rate = []
             MarketHistory_Price = []
             MarketHistory_Quantity = []
             MarketHistory_FillType_Z = []  # -1/1 encodecd
             MarketHistory_OrderType_Z = []
             Sell_Active_volum = []
             Buy_Active_volum = []
             Total_Active_Volum = []
             Historical_Volum = []
             depth = 100
             global Tick_Ask
             global Tick_Bid
             global Tick_Last
             global OrderBook_buy_Rate
             global OrderBook_buy_Quantity
             global OrderBook_sell_Quantity
             global OrderBook_sell_Rate
             global MarketHistory_Price
             global MarketHistory_Quantity
             global MarketHistory_FillType_Z
             global MarketHistory_OrderType_Z
             global Sell_Active_volum
             global Buy_Active_volum
             global Total_Active_Volum
             global Historical_Volum
             with futures.ThreadPoolExecutor(max_workers=5) as ex:
                 # print('main: starting')
                 ex.submit(self._Extract_OrderBook_Buy)
                 ex.submit(self._Extract_OrderBook_Sell)
                 ex.submit(self._Extract_Tick)
                 ex.submit(self._Extract_MarketHistory)

             # Clean Up
             # -------------------------#
             del json_data
             # -------------------------#
             self.Last_index = len(Tick_Last)

             MarketHistory_Price = np.reshape(MarketHistory_Price, (int(len(MarketHistory_Price) / depth), depth))
             MarketHistory_Quantity = np.reshape(MarketHistory_Quantity,
                                                 (int(len(MarketHistory_Quantity) / depth), depth))
             MarketHistory_FillType_Z = np.reshape(MarketHistory_FillType_Z,
                                                   (int(len(MarketHistory_FillType_Z) / depth), depth))
             MarketHistory_OrderType_Z = np.reshape(MarketHistory_OrderType_Z,
                                                    (int(len(MarketHistory_OrderType_Z) / depth), depth))
             OrderBook_buy_Quantity = np.reshape(OrderBook_buy_Quantity,
                                                 (int(len(OrderBook_buy_Quantity) / depth), depth))
             OrderBook_buy_Rate = np.reshape(OrderBook_buy_Rate, (int(len(OrderBook_buy_Rate) / depth), depth))
             OrderBook_sell_Quantity = np.reshape(OrderBook_sell_Quantity,
                                                  (int(len(OrderBook_sell_Quantity) / depth), depth))
             OrderBook_sell_Rate = np.reshape(OrderBook_sell_Rate, (int(len(OrderBook_sell_Rate) / depth), depth))
             Buy_Active_volum = np.reshape(Buy_Active_volum, (1, len(Buy_Active_volum)))
             Sell_Active_volum = np.reshape(Sell_Active_volum, (1, len(Sell_Active_volum)))
             Historical_Volum = np.reshape(Historical_Volum, (1, len(Historical_Volum)))
             Total_Active_Volum = Buy_Active_volum + Sell_Active_volum
             Tick_Ask = np.reshape(Tick_Ask, (1, len(Tick_Ask)))
             Tick_Bid = np.reshape(Tick_Bid, (1, len(Tick_Bid)))
             Tick_Last = np.reshape(Tick_Last, (1, len(Tick_Last)))

             Tick_Ask_Norm = self.Tick_Ask_Scaler.fit_transform(
                 Tick_Ask.T).T  # we rotat the  input to normalize over the time aixs and re rotate it to be sutale for concatination in the state matrix
             Tick_Bid_Norm = self.Tick_Bid_Scaler.fit_transform(Tick_Bid.T).T
             Tick_Last_Norm = self.Tick_Last_Scaler.fit_transform(Tick_Last.T).T
             OrderBook_buy_Rate_Norm = self.OrderBook_buy_Rate_Scaler.fit_transform(OrderBook_buy_Rate)
             OrderBook_sell_Quantity_Norm = self.OrderBook_sell_Quantity_Scaler.fit_transform(OrderBook_sell_Quantity)
             OrderBook_sell_Rate_Norm = self.OrderBook_sell_Rate_Scaler.fit_transform(OrderBook_sell_Rate)
             MarketHistory_Price_Norm = self.MarketHistory_Price_Scaler.fit_transform(MarketHistory_Price)
             MarketHistory_Quantity_Norm = self.MarketHistory_Quantity_Scaler.fit_transform(MarketHistory_Quantity)
             OrderBook_buy_Quantity_Norm = self.OrderBook_buy_Quantity_Scaler.fit_transform(OrderBook_buy_Quantity)
             Sell_Active_volum_Norm = self.Sell_Active_volum_Scaler.fit_transform(Sell_Active_volum.T).T
             Buy_Active_volum_Norm = self.Buy_Active_volum_Scaler.fit_transform(Buy_Active_volum.T).T
             Total_Active_Volum_Norm = self.Total_Active_Volum_Scaler.fit_transform(Total_Active_Volum.T).T
             Historical_Volum_Norm = self.Historical_Volum_Scaler.fit_transform(Historical_Volum.T).T

             Tick_Ask_Norm = self._repeatN(Tick_Ask_Norm, 100)
             Tick_Bid_Norm = self._repeatN(Tick_Bid_Norm, 100)
             Tick_Last_Norm = self._repeatN(Tick_Last_Norm, 100)
             Buy_Active_volum_Norm = self._repeatN(Buy_Active_volum_Norm, 100)
             Sell_Active_volum_Norm = self._repeatN(Sell_Active_volum_Norm, 100)
             Historical_Volum_Norm = self._repeatN(Historical_Volum_Norm, 100)
             Total_Active_Volum_Norm = self._repeatN(Total_Active_Volum_Norm, 100)

             self.OrderBook_buy_Rate = OrderBook_buy_Rate
             self.OrderBook_sell_Quantity = OrderBook_sell_Quantity
             self.OrderBook_sell_Rate = OrderBook_sell_Rate
             self.OrderBook_buy_Quantity = OrderBook_buy_Quantity
             self.Tick_Last = Tick_Last.T

             '''
             self.Tick_Ask1                  = Tick_Ask
             self.Tick_Ask                   = Tick_Ask_Norm             
             self.Tick_Bid                   = Tick_Bid_Norm
             self.Tick_Last                  = Tick_Last_Norm
             self.MarketHistory_Price        = MarketHistory_Price_Norm
             self.MarketHistory_Quantity     = MarketHistory_Quantity_Norm
             self.MarketHistory_FillType_Z   = MarketHistory_FillType_Z #ZERO encodecd
             self.MarketHistory_OrderType_Z  = MarketHistory_OrderType_Z
             self.Sell_Active_volum          = Sell_Active_volum_Norm
             self.Buy_Active_volum           = Buy_Active_volum_Norm
             self.Total_Active_Volum         = Total_Active_Volum_Norm
             self.Historical_Volum           = Historical_Volum_Norm
             '''
             Input_Feturs = np.concatenate((
                 # Tick_Ask_Norm,
                 # Tick_Bid_Norm,
                 # Tick_Last_Norm,
                 OrderBook_buy_Rate_Norm,
                 OrderBook_buy_Quantity_Norm,
                 OrderBook_sell_Quantity_Norm,
                 OrderBook_sell_Rate_Norm
                 # MarketHistory_Price_Norm,
                 # MarketHistory_Quantity_Norm,
                 # MarketHistory_FillType_Z,
                 # MarketHistory_OrderType_Z,
                 # Buy_Active_volum_Norm,
                 # Sell_Active_volum_Norm,
                 # Historical_Volum_Norm,
                 # Total_Active_Volum_Norm

             ), axis=1)
             self.EnvStats = np.reshape(Input_Feturs, [Input_Feturs.shape[0], int(Input_Feturs.shape[1] / 20), 20])

             # Clean Up
             # ----------------------------------#
             # del MarketHistory_Price
             # del MarketHistory_Quantity
             # del MarketHistory_FillType_Z  #-1/1 encodecd
             # del MarketHistory_OrderType_Z
             # del OrderBook_buy_Quantity
             # del OrderBook_buy_Rate
             # del OrderBook_sell_Quantity
             # del OrderBook_sell_Rate
             # del Tick_Ask
             # del Tick_Bid
             # del Tick_Last
             # del Sell_Active_volum
             # del Buy_Active_volum
             # del Total_Active_Volum
             # del Historical_Volum
             del Input_Feturs
             # del MarketHistory_Price_Norm
             # del MarketHistory_Quantity_Norm
             del OrderBook_buy_Quantity_Norm
             del OrderBook_buy_Rate_Norm
             del OrderBook_sell_Quantity_Norm
             del OrderBook_sell_Rate_Norm
             # del Tick_Ask_Norm
             # del Tick_Bid_Norm
             # del Tick_Last_Norm
             # del Sell_Active_volum_Norm
             # del Buy_Active_volum_Norm
             # del Total_Active_Volum_Norm
             # del Historical_Volum_Norm
        # --------------------------------------#




        def _Extract_OrderBook_Sell(self):
            global OrderBook_sell_Rate
            global OrderBook_sell_Quantity
            #print('sell: starting')
            global json_data
            for i in range(len(json_data)):

                if ((json_data[i] != [] or None) and
                        (json_data[i]['Tick'] != [] or None) and
                        (json_data[i]['OrderBook'] != [] or None) and
                        (json_data[i]['MarketHistory'] != [] or None) and
                        (json_data[i]['Tick']['result'] is not None) and
                        (json_data[i]['OrderBook']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['buy'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['sell'] != [] or None) and
                        (json_data[i]['Tick']['result'] != [] or None) and
                        (json_data[i]['Tick']['result']['Ask'] != [] or None) and
                        (json_data[i]['Tick']['result']['Bid'] != [] or None) and
                        (json_data[i]['Tick']['result']['Last'] != [] or None)
                ):


                    for m in range(len((json_data[1]['OrderBook']['result']['sell']))):
                        OrderBook_sell_Quantity.append(json_data[i]['OrderBook']['result']['sell'][m]['Quantity'])
                        OrderBook_sell_Rate.append(json_data[i]['OrderBook']['result']['sell'][m]['Rate'])
                    #Sell_Active_volum.append(sum(OrderBook_sell_Rate))


        def _Extract_OrderBook_Buy(self):
            #print('buy: starting')
            global OrderBook_buy_Rate
            global OrderBook_buy_Quantity
            global json_data
            for i in range(len(json_data)):

                if ((json_data[i] != [] or None) and
                        (json_data[i]['Tick'] != [] or None) and
                        (json_data[i]['OrderBook'] != [] or None) and
                        (json_data[i]['MarketHistory'] != [] or None) and
                        (json_data[i]['Tick']['result'] is not None) and
                        (json_data[i]['OrderBook']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['buy'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['sell'] != [] or None) and
                        (json_data[i]['Tick']['result'] != [] or None) and
                        (json_data[i]['Tick']['result']['Ask'] != [] or None) and
                        (json_data[i]['Tick']['result']['Bid'] != [] or None) and
                        (json_data[i]['Tick']['result']['Last'] != [] or None)
                ):

                    for m in range(len((json_data[1]['OrderBook']['result']['buy']))):
                        OrderBook_buy_Quantity.append(json_data[i]['OrderBook']['result']['buy'][m]['Quantity'])
                        OrderBook_buy_Rate.append(json_data[i]['OrderBook']['result']['buy'][m]['Rate'])
                    #Buy_Active_volum.append(sum(OrderBook_buy_Rate))

        def _Extract_MarketHistory(self):
            #print('history: starting')
            global MarketHistory_Price
            global MarketHistory_Quantity
            global MarketHistory_FillType_Z
            global MarketHistory_OrderType_Z
            global MarketHistory_OrderType_Z
            global Sell_Active_volum
            global Buy_Active_volum
            global Total_Active_Volum
            global Historical_Volum
            global json_data
            for i in range(len(json_data)):

                if ((json_data[i] != [] or None) and
                        (json_data[i]['Tick'] != [] or None) and
                        (json_data[i]['OrderBook'] != [] or None) and
                        (json_data[i]['MarketHistory'] != [] or None) and
                        (json_data[i]['Tick']['result'] is not None) and
                        (json_data[i]['OrderBook']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['buy'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['sell'] != [] or None) and
                        (json_data[i]['Tick']['result'] != [] or None) and
                        (json_data[i]['Tick']['result']['Ask'] != [] or None) and
                        (json_data[i]['Tick']['result']['Bid'] != [] or None) and
                        (json_data[i]['Tick']['result']['Last'] != [] or None)
                ):


                    for m in range(len((json_data[1]['MarketHistory']['result']))):
                        MarketHistory_Price.append(json_data[i]['MarketHistory']['result'][m]['Price'])
                        MarketHistory_Quantity.append(json_data[i]['MarketHistory']['result'][m]['Quantity'])

                        if (json_data[i]['MarketHistory']['result'][m]['FillType']) == 'PARTIAL_FILL':
                            MarketHistory_FillType_Z.append(-1)
                        elif (json_data[i]['MarketHistory']['result'][m]['FillType']) == 'FILL':
                            MarketHistory_FillType_Z.append(1)
                        if (json_data[i]['MarketHistory']['result'][m]['OrderType']) == 'BUY':
                            MarketHistory_OrderType_Z.append(-1)
                        elif (json_data[i]['MarketHistory']['result'][m]['OrderType']) == 'SELL':
                            MarketHistory_OrderType_Z.append(1)
                    Historical_Volum.append(sum(MarketHistory_Quantity))

        def _Extract_Tick(self):
            #print('Tick: starting')
            global Tick_Ask
            global Tick_Bid
            global Tick_Last
            global json_data
            for i in range(len(json_data)):

                if ((json_data[i] != [] or None) and
                        (json_data[i]['Tick'] != [] or None) and
                        (json_data[i]['OrderBook'] != [] or None) and
                        (json_data[i]['MarketHistory'] != [] or None) and
                        (json_data[i]['Tick']['result'] is not None) and
                        (json_data[i]['OrderBook']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] is not None) and
                        (json_data[i]['MarketHistory']['result'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['buy'] != [] or None) and
                        (json_data[i]['OrderBook']['result']['sell'] != [] or None) and
                        (json_data[i]['Tick']['result'] != [] or None) and
                        (json_data[i]['Tick']['result']['Ask'] != [] or None) and
                        (json_data[i]['Tick']['result']['Bid'] != [] or None) and
                        (json_data[i]['Tick']['result']['Last'] != [] or None)
                ):

                    #Tick_Ask.append(json_data[i]['Tick']['result']['Ask'])
                    #Tick_Bid.append(json_data[i]['Tick']['result']['Bid'])
                    Tick_Last.append(json_data[i]['Tick']['result']['Last'])
                    #print(Tick_Last[-1])


        def _repeatN(self ,arr , n ):
                x1 = np.repeat(arr.T,n)
                x2 = np.reshape(x1,[arr.T.shape[0],n])
                return x2
