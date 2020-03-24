


class TradingEnv_Logic():
    def __init__(self, market_data):
        self.Index = 0
        self.Cash = [0]
        self.Coin = [0]
        self.Gain = [0]
        self.Reward = [0]
        self.AccGain = [0]
        self.AccReward = [0]
        self.MarketGain = [0]
        self.Volatility = [0]
        self.Action = 0
        self.vwindow = 20  # volatility window
        self.NoOfActions = 21
        self.FillType = [0]
        self.Actionspace = np.linspace(-1, 1, num=self.NoOfActions)
        self.EffictiveRate = [0]  # -ve for sell orders +ve for buy orders zero for no action
        self.Tradingfee = 0.01
        self.Market_Data = market_data
        self.Action_log = []
        self.Volum_log = []
        self.Last_Significant_Action = np.array(
            [0, 0, 0, 0, 0, 0])  # contains (coin , cash , Effrate , action , fillType , index)  action != 0 ,-1 to 1
        self.TotalCash = []
        self.RlativeGain = []
        json_data = []

    def _ActionGain(self):
        if self.Action != 0:
            # index = self.Index  ;
            coin = self.Last_Significant_Action[:, 0];
            cash = self.Last_Significant_Action[:, 1];
            rate = self.Last_Significant_Action[-1, 2]
            cashgain = (cash[-1] - cash[-2] * (1 + self.Tradingfee)) / (
                        cash[-1] + 0.000000000005)  # to avoid devion by zero small nuber added
            if abs(cashgain) > 10:
                cashgain = 0
            coingain = (coin[-1] - coin[-2] * (1 + self.Tradingfee)) / (coin[-1] + 0.000000000005)
            if abs(coingain) > 10:
                coingain = 0
            cashcoingain = ((cash[-1] - cash[-2] * (1 + self.Tradingfee)) + (
                        coin[-1] - coin[-2] * (1 + self.Tradingfee)) * rate) / (
                                       cash[-2] + coin[-2] * rate + 0.0000000005)
            if abs(cashcoingain) > 10:
                cashcoingain = 0
            totalGain = cashcoingain  # + cashgain + coingain

        else:
            totalGain = 0
        self.Gain.append(totalGain)
        self.AccGain.append(self.AccGain[-1] + totalGain)
        return totalGain

    def _GetMarketGain(self):
        if self.Action != 0:
            last_action_Index = int(self.Last_Significant_Action[-2, 5])
        else:
            last_action_Index = int(self.Last_Significant_Action[-1, 5])

        currindex = self.Index;
        rate = self.Tick_Last[:]  # shouled be calculated from the original data
        MarketGain = ((rate[currindex] - rate[last_action_Index]) / rate[currindex])
        self.MarketGain.append(MarketGain)
        return MarketGain

    def _GetVolatility(self):
        # calculat volatility
        LocalVolatility = 0
        windw = self.vwindow  # volatility window
        # TickPriceIndex  = 2  #index of Tick_Last in EnvStats matrix # edited to avoid -ves in rates
        if self.Index > windw:
            LocalVolatility = np.std(self.Tick_Last[(
                                                                self.Index - windw):self.Index])  # (self.Index-windw):self.Index ,TickPriceIndex,0]) # usinf index 0 becouse the value is repeatimested 100  # Discarded Not Using Envstats
        else:
            LocalVolatility = np.std(self.Tick_Last[:self.Index])
        self.Volatility.append(LocalVolatility)
        return LocalVolatility

    def _CalculateReward(self):
        if self.Index != 0:
            ActionGain = self._ActionGain()
            MarketGain = 0  # self._GetMarketGain()
            volaility = 0  # self._GetVolatility()
            # bahaviorenforce = self._BehaviorAssit( ) # to be implemented to encourge or discarge behavior
            reward = (ActionGain - 0.5 * MarketGain - 0.5 * volaility) + 0.1 * self.FillType[self.Index] * abs(
                (ActionGain - 0.5 * MarketGain - 0.5 * volaility))  # inportant To revize
        else:
            reward = 0.000005
        reward = reward / 1000
        self.Reward.append(reward)
        self.AccReward.append(self.AccReward[-1] + reward)
        return reward

    def _BuyCoin(self, cvol):  # cvol is cash volum
        FillType = 1  # -1 is partially filled 1 is completrly filled
        if round(cvol, 5) != 0:  # to avoid near zero float error

            # there is an uber limet on the bougt coin becouse of the orderbook depth so we shouled chek the avlability of the volum firist
            # assert (np.sum(self.OrderBook_sell_Quantity[self.Index,:]) > vol ), "exceptin buy  avalable vol "
            i = -1
            AvVol = self.OrderBook_sell_Quantity[self.Index, :]
            AvRate = self.OrderBook_sell_Rate[self.Index, :]
            xvol = np.sum(self.OrderBook_sell_Rate[self.Index, :] * self.OrderBook_sell_Quantity[self.Index,
                                                                    :])  # avelable volum in this tick (curent orderbook) in cash
            AccVol = 0
            DistVol = []
            DistRate = []
            if cvol > xvol:
                cvol = xvol
                DistVol = AvVol
                DistRate = AvRate
                FillType = -1
            else:
                index = 0
                while round(cvol, 5) != 0:
                    DistRate.append(AvRate[index])
                    if cvol > AvVol[index] * AvRate[index]:
                        DistVol.append(AvVol[index])
                        cvol = cvol - AvVol[index] * AvRate[index]
                    else:
                        DistVol.append(cvol / AvRate[index])
                        cvol = 0
                    index += 1
                    AccVol += (DistRate[-1] * DistVol[-1])

            DistVol = np.array(DistVol)
            DistRate = np.array(DistRate)
            effRate = np.sum(DistVol * DistRate) / np.sum(DistVol)
            coin = self.Coin[self.Index] + np.sum(DistVol)
            coin = coin - self.Coin[self.Index - 1] * self.Tradingfee  # taking The trading fee
            cash = round(self.Cash[self.Index] - float(np.sum(DistVol * DistRate)), 5)
        else:
            coin, cash, effRate = self.Coin[self.Index], self.Cash[self.Index], 0
        return coin, cash, effRate, FillType  # taking the trading fee

    def _SellCoin(self, vol):
        # there is an uber limet on the bougt coin becouse of the orderbook depth so we shouled chek the avlability of the volum firist
        FillType = 1  # -1 is partially filled 1 is completrly filled
        if round(vol, 5) != 0:  # to avoid near zero float error
            i = -1
            AvVol = self.OrderBook_buy_Quantity[self.Index, :]
            AvRate = self.OrderBook_buy_Rate[self.Index, :]
            AccVol = 0
            DistVol = []
            DistRate = []
            xvol = np.sum(self.OrderBook_buy_Quantity[self.Index, :])  # avelable volum in this tick (curent orderbook)
            if vol > xvol:
                vol = xvol
                FillType = -1
                DistVol = AvVol
                DistRate = AvRate
            else:
                while AccVol < vol:
                    i += 1
                    AccVol = AccVol + AvVol[i]
                    DistRate.append(AvRate[i])

                for index in range(len(DistRate)):
                    if vol > AvVol[index]:
                        DistVol.append(AvVol[index])
                        vol = vol - AvVol[index]
                    else:
                        DistVol.append(vol)

            DistVol = np.array(DistVol)
            DistRate = np.array(DistRate)
            effRate = np.sum(DistVol * DistRate) / np.sum(DistVol)
            coin = round(self.Coin[self.Index] - np.sum(DistVol), 5)
            cash = self.Cash[self.Index] + (np.sum(DistVol) * effRate)
            cash = cash - self.Cash[-1] * self.Tradingfee  # taking the trading fee


        else:
            coin, cash, effRate = self.Coin[self.Index], self.Cash[self.Index], 0

        return coin, cash, effRate, FillType

    def _TakeAction(self):
        # action logic
        # does not take in account unishial 0 cash or coin
        if round(self.Action, 3) > 0:
            self.Volum_log.append((abs(self.Action * self.Cash[self.Index]), 'cash'))
            coin, cash, effRate, FillType = self._BuyCoin(abs(self.Action * self.Cash[self.Index]))
            self.Last_Significant_Action = np.vstack(
                (self.Last_Significant_Action, [coin, cash, effRate, self.Action, FillType, self.Index]))

        elif round(self.Action, 3) < 0:
            self.Volum_log.append((abs(self.Action * self.Coin[self.Index]), 'coin'))
            coin, cash, effRate, FillType = self._SellCoin(abs(self.Action * self.Coin[self.Index]))
            self.Last_Significant_Action = np.vstack(
                (self.Last_Significant_Action, [coin, cash, effRate, self.Action, FillType, self.Index]))
        else:
            self.Volum_log.append((0, 'No action '))
            effRate = 0
            coin = self.Coin[self.Index]
            cash = self.Cash[self.Index]
            FillType = 0
        # update cash and coin and EffictiveRate
        self.Coin.append(coin)  # adjusting for market tradfing fee
        self.Cash.append(cash)  # adjusting for market tradfing fee
        self.EffictiveRate.append(effRate)
        self.FillType.append(FillType)

    def _GetPerformanceMetrics(self):
        # make PerformanceMetrics(
        if self.Index > 0:
            coin = self._repeatN(
                np.array([(self.Coin[self.Index] - self.Coin[self.Index - 1]) / self.Coin[self.Index - 1]]), 100)
            cash = self._repeatN(
                np.array([(self.Cash[self.Index] - self.Cash[self.Index - 1]) / self.Cash[self.Index - 1]]), 100)
            gain = self._repeatN(
                np.array([(self.Gain[self.Index] - self.Gain[self.Index - 1]) / self.Gain[self.Index - 1]]), 100)
            accgain = self._repeatN(
                np.array([(self.AccGain[self.Index] - self.AccGain[self.Index - 1]) / self.AccGain[self.Index - 1]]),
                100)
            reward = self._repeatN(
                np.array([(self.Rward[self.Index] - self.Rward[self.Index - 1]) / self.Rward[self.Index - 1]]), 100)
            accreward = self._repeatN(np.array(
                [(self.AccReward[self.Index] - self.AccReward[self.Index - 1]) / self.AccReward[self.Index - 1]]), 100)
            volatility = self._repeatN(np.array(
                [(self.Volatility[self.Index] - self.Volatility[self.Index - 1]) / self.Volatility[self.Index - 1]]),
                                       100)

        else:
            coin = self._repeatN(np.array([0]), 100)
            cash = self._repeatN(np.array([1]), 100)
            gain = self._repeatN(np.array([0]), 100)
            accgain = self._repeatN(np.array([0]), 100)
            reward = self._repeatN(np.array([0]), 100)
            accreward = self._repeatN(np.array([0]), 100)
            volatility = self._repeatN(np.array([0]), 100)

        PerformanceMetrics = np.concatenate((
            coin,
            cash,
            gain,
            accgain,
            reward,
            accreward,
            volatility
        ), axis=0)

        return PerformanceMetrics

    def _step(self, action):
        self.Action = action
        self._TakeAction()
        reward = self._CalculateReward()
        return reward

    def _reset(self):
        self.Index = 0
        self.Cash = [self.Cash[0]]
        self.Coin = [self.Coin[0]]
        self.Gain = [0]
        self.Reward = [0]
        self.AccGain = [0]
        self.AccReward = [0]
        self.Volatility = [0]
        self.EffictiveRate = [0]
        self.MarketGain = [0]
        self.Action = 0
        self.FillType = [0]

    # interface with external codes
    def new_episode(self):
        self._reset()

    def get_state(self):
        '''
        perfMetrices = self._GetPerformanceMetrics()
        State = np.concatenate((
                    self.EnvStats[self.Index],
                    perfMetrices
                      ),axis = 0)
        return State
        '''
        state = self.EnvStats[self.Index]
        prevstate = self.EnvStats[self.Index - 1]
        obs = (prevstate) - (state)
        if self.Index > 0:
            obs = np.reshape(obs, -1)
        else:
            obs = state

        return obs

    @property
    def is_episode_finished(self):
        finshed = False
        if self.Index >= self.Last_index - 1 or (
                round(self.Cash[self.Index], 5) + round(self.Coin[self.Index], 5)) == 0:
            finshed = True

        return finshed

    def make_action(self, action):  # self.actions[a]
        self.Action_log.append(action)
        r = self._step(action)  # shouled retern reward
        self.Index += 1
        return r

    def set_coin(self, coin):
        self.Coin[0] = coin

    def set_cash(self, cash):
        self.Cash[0] = cash

        self.Cash[0] = cash

    # ---------------------gyme interface----------------#

    def step(self, action):
        action = action.argmax()
        action = self.Actionspace[action]
        reward = self.make_action(action)
        obs = self.get_state()
        done = self.is_episode_finished
        info = self.AccGain[-1]
        self.TotalCash.append(self.Coin[-1] * self.Tick_Last[self.Index] + self.Cash[-1])
        self.RlativeGain.append(self.TotalCash[-1] / self.IntialCash)

        return obs, reward, done, info

    def reset(self):

        self._reset()
        self.set_cash(float(np.random.randint(10, 1000)))
        self.set_coin(float((np.random.randint(10, 1000)) / self.Tick_Last[0]))
        self.IntialCash = self.Coin[-1] * self.Tick_Last[0] + self.Cash[-1]
        return self.get_state()

    def render(self, mode='human', close=False):

        pass

    def config(self, cash, coin):
        self.set_coin(coin)
        self.set_cash(cash)