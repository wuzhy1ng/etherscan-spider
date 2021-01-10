# etherscan-spider
Ethereum data spider on etherscan

## Targets

- Crawl transaction data from etherscan api

## Dependency
- Python3.5 or higher version
- Scrapy1.7 or higher version

## Set seeds of crawl task
Crawling transaction data need a seed address or a seed file
It's highly recommand you prepared a seed file with `csv` format in `./data` folder
Just like this:`./data/seed.csv`:
```
0x000000000532b45f47779fce440748893b257865,phish-hack
0x0000000009324b6434d7766af41908e4c49ee1d7,phish-hack
0x00000000219ab540356cbb839cbe05303d7705fa,phish-hack
...
```

## Set your etherscan api token
Crawling task needs etherscan api tokens. 
You can add your api tokens in `settings.py`
```
# etherscan apikey
APITOKENS = [
  
]
```

## Set proxy port
If you are located in China, it's necessary for you to make a proxy for visit etherscan.
I'm prepared the middleware for `SSR`, you can see at `settings.py`:
```
DOWNLOADER_MIDDLEWARES = {
    'etherscan_spider.middlewares.EtherscanSpiderDownloaderMiddleware': 543,
}
```
For more detail of this `DownloadMiddleware`:
```    
def process_request(self, request, spider):
  request.meta['proxy'] = "http://localhost:1080"
  return None
```
You can set `SSR` port on 1080 and start spider as usual.

## Crawl transaction with a strategy
There are three kinds of strategies for you to start crawl transaction data,
including  `Random`, `BFS` and `OPICHaircut`.
You can start a spider with `BFS` strategy on console:
```
scrapy crawl bfs_tx_spider -a file=./data/seed.csv -a depth=2
```
In this way, spider will start crawl from seed address of file and the depth within 2 floors.
You can start a spider with `BFS` strategy and control the extend count equals to 300 just like this:
```
scrapy crawl bfs_tx_spider -a file=./data/seed.csv -a epa=300
```
For more usage, please consult the author.

## Data storage
You can find spider output `csv` file in `./data` folder with specific stragety, for example, if you start a `BFS` spider, the output folder is `./data/BFS`.
It's looks lick this:
```
hash,from,to,value,blockNumber,timeStamp,gas,gasPrice,gasUsed
0x94917b89296051b066db2ac572987d8ec48a88716f51291a47d50e6b1e8cc20c,0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be,0x0a0ba956038d4a66002d612648332b9c4ab7646c,500000000000000000,6026742,1532511199,21000,60000000000,21000
0x5c992599647f4a95919d68c4fe137612635bb9c9530b6a4572049c6197678a81,0x26b315a3dd31f4002df033b5e493c05cdbd9d36c,0x0a0ba956038d4a66002d612648332b9c4ab7646c,950209050000000000,6030488,1532565565,21000,2000000000,21000
...
```
