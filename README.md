# etherscan-spider
Ethereum data spider on etherscan

## Targets

- Crawl transaction data from etherscan

## Dependency
- Python3.5 or higher version
- Scrapy1.7 or higher version

## Set seeds of crawl task
Crawling transaction data need a seed address or a seed file
It's highly recommand you prepared a seed file with `csv` format in `./data` fold
Just like this:`./data/seed.csv`:
> 0x000000000532b45f47779fce440748893b257865,phish-hack
> 0x0000000009324b6434d7766af41908e4c49ee1d7,phish-hack
> 0x00000000219ab540356cbb839cbe05303d7705fa,phish-hack
> ...

## Set your etherscan api token
Crawling task needs etherscan api tokens. 
You can add your api tokens in `settings.py`
```
# etherscan apikey
APITOKENS = [
  
]
```

## Crawl transaction with a strategy

