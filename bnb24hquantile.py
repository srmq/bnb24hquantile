#!/usr/bin/env python3

# bnb24hquantile.py
#     Copyright (C) 2021  Sergio Queiroz <srmq@srmq.org>

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU Affero General Public License as
#     published by the Free Software Foundation, either version 3 of the
#     License, or (at your option) any later version.

#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.

#     You should have received a copy of the GNU Affero General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

import httpx
import asyncio
import time
import bisect
import json

class TickerChangeInfo:
    def __init__(self, symbolInfo):
        self.changesIn24h = {}
        self.symbolsFailed = {}
        self.symbolInfo = {}
        for tradingPair in symbolInfo:
            if tradingPair['status'] == 'TRADING':
                self.symbolInfo[tradingPair['symbol']] = tradingPair
    
    def addResult(self, result):
        self.changesIn24h[result['symbol']] = result

    def addFailure(self, failedSymbol, exc):
        self.symbolsFailed[failedSymbol] = exc

    def flattenedResults(self):
        results = []
        for key in self.changesIn24h:
            s = self.changesIn24h[key].copy()
            for infokey in self.symbolInfo[key]:
                if s.get(infokey) is None:
                    s[infokey] = (self.symbolInfo[key])[infokey]
            results.append(s)
        return results
            

class RateLimiter:
    intervalMultiplier = {
        'SECOND': 1,
        'MINUTE': 60,
        'DAY': 86400
    }

    async def addWeightAndRequest(self, weightToAdd, nRequestToAdd):
        async with self.lock:
            limitForWeight = self.limitDict.get('REQUEST_WEIGHT')
            respectWeightLimits = True
            if limitForWeight is not None:
                for weightLimit in limitForWeight['limitList']:
                    eachsecs = weightLimit['eachsecs']
                    limitForSecs = weightLimit['limit']
                    goBackTime = time.time() - eachsecs - 1
                    n = bisect.bisect_left(limitForWeight['requests'], goBackTime)
                    n = len(limitForWeight['requests']) - n
                    if (n + weightToAdd) >= limitForSecs:
                        respectWeightLimits = False
                        return False

            limitForRequests = self.limitDict.get('RAW_REQUESTS')
            respectRawLimits = True
            if limitForRequests is not None:
                for rawLimit in limitForRequests['limitList']:
                    eachsecs = rawLimit['eachsecs']
                    limitForSecs = rawLimit['limit']
                    goBackTime = time.time() - eachsecs - 1
                    n = bisect.bisect_left(limitForRequests['requests'], goBackTime)
                    n = len(limitForRequests['requests']) - n
                    if (n + nRequestToAdd) >= limitForSecs:
                        respectRawLimits = False
                        return False
            assert (respectWeightLimits and respectRawLimits)
            if limitForWeight is not None:
                for i in range(0, weightToAdd):
                    limitForWeight['requests'].append(time.time())
                while(len(limitForWeight['requests']) > limitForWeight['maxlen']):
                    limitForWeight['requests'].pop(0)
            if limitForRequests is not None:
                for i in range(0, nRequestToAdd):
                    limitForRequests['requests'].append(time.time())
                while(len(limitForRequests['requests']) > limitForRequests['maxlen']):
                    limitForRequests['requests'].pop(0)
        return True

    def updateRateLimits(self, rateLimits):
        self.limitDict = {}
        for rateLimit in rateLimits:
            rateType = rateLimit['rateLimitType']
            limitForRate = self.limitDict.get(rateType)
            if (limitForRate is None):
                limitForRate = {'maxlen': -1, 'limitList': [], 'requests': []}
                self.limitDict[rateType] = limitForRate
            newlimit = {
                'eachsecs': RateLimiter.intervalMultiplier[rateLimit['interval']]*rateLimit['intervalNum'], 
                'limit': rateLimit['limit']}
            limitForRate['limitList'].append(newlimit)
            if (newlimit['limit'] > limitForRate['maxlen']):
                limitForRate['maxlen'] = newlimit['limit']

    def __init__(self, exchangeInfo):
        self.updateRateLimits(exchangeInfo['rateLimits'])
        self.lock = asyncio.Lock()


def getSpotURL():
    return 'https://api.binance.com'


async def getExchangeInfo():
    spotExchangeInfoURL = getSpotURL() + "/api/v3/exchangeInfo"
    async with httpx.AsyncClient() as client:
        r = await client.get(spotExchangeInfoURL)
    return r.json()

async def get24hTickerChange(symbol, tickerChangeInfos):
    url = getSpotURL() + "/api/v3/ticker/24hr"
    params = {'symbol': symbol}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, timeout=30.0)
        try:
            r.raise_for_status()
            tickerChangeInfos.addResult(r.json())
            return r.json()
        except httpx.HTTPError as exc:
            tickerChangeInfos.addFailure(symbol, exc)
            return {'symbol': symbol, 'error': exc.response.status_code}


def got24hResult(future):
    print(future.result())

async def run24TicherChangeWithLimiter(coros, rateLimiter, maxnum = 50):
    #will affect REQUEST_WEIGHT and RAW_REQUESTS
    runningTasks = []
    firstRun = min(len(coros), maxnum)
    for i in range(0, firstRun):
        # só devo chegar aqui depois de ver que está ok quanto a maxnum
        while (not await rateLimiter.addWeightAndRequest(1, 1)):
            await asyncio.sleep(0)
        task = asyncio.ensure_future(coros[i])
        task.add_done_callback(got24hResult)
        runningTasks.append(task)
    if (firstRun < len(coros)):
        i = firstRun
        while i < len(coros):
            await asyncio.sleep(0)
            for t in runningTasks:
                if (t.done()):
                    runningTasks.remove(t)
                    while (not await rateLimiter.addWeightAndRequest(1, 1)):
                        await asyncio.sleep(0)
                    task = asyncio.ensure_future(coros[i])
                    task.add_done_callback(got24hResult)
                    runningTasks.append(task)
                    i = i + 1
                    break
    while (len(runningTasks)) > 0:
        await asyncio.sleep(0)
        for t in runningTasks:
            if (t.done()):
                runningTasks.remove(t)
        


async def main():
    exchangeInfo = await getExchangeInfo()
    rateLimiter = RateLimiter(exchangeInfo)

    tradingSymbols = []
    for s in exchangeInfo['symbols']:
        if (s['status'] == 'TRADING'):
            tradingSymbols.append(s['symbol'])

    tickerChangeInfos = TickerChangeInfo(exchangeInfo['symbols'])
    
    getSymbolsCoros = [get24hTickerChange(symbol, tickerChangeInfos) for symbol in tradingSymbols]

    await run24TicherChangeWithLimiter(getSymbolsCoros, rateLimiter)
    await asyncio.sleep(1)

    print("\n DONE! Now writing files...")

    with open("ticketChange24h.json", "w") as resultFile:
        tickerInfo = []
        json.dump(tickerChangeInfos.flattenedResults(), resultFile)

    with open("ticketChange24h-errors.json", "w") as resultFile:
        json.dump(tickerChangeInfos.symbolsFailed, resultFile)
    

if __name__ == "__main__":
    asyncio.run(main())
