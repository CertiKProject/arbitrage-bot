

from web3 import Web3, HTTPProvider

class CHAIN:
    BSC = "BSC"
    ETH = "ETH"
    POLYGON = "POLYGON"
    BSC_ARCHIVE = "BSC_ARCHIVE"
    ETH_ARCHIVE = "ETH_ARCHIVE"

ARCHIVE_VERSION = {
    CHAIN.BSC: CHAIN.BSC_ARCHIVE,
    CHAIN.ETH: CHAIN.ETH_ARCHIVE,
    CHAIN.POLYGON: CHAIN.POLYGON,
    None: None
}

CHAIN_NODE_URLS = {
    CHAIN.BSC: 'https://late-divine-shape.bsc.quiknode.pro/b8b9f12c1883a9483d1302f9b139872e4ed29532/',
    CHAIN.ETH: 'https://eth-full-node.certik-skynet.com/',
    CHAIN.POLYGON: 'https://polygon-mainnet.g.alchemy.com/v2/tTo5OoITZ8QB1RQ3j7oplK_NQO7FaJZ8/',
    CHAIN.BSC_ARCHIVE: 'https://bsc-node.certik-skynet.com/',
    CHAIN.ETH_ARCHIVE: 'https://eth-node.certik-skynet.com/',
    None: None
}

W3S = {
    CHAIN.BSC: Web3(HTTPProvider(CHAIN_NODE_URLS[CHAIN.BSC])),
    CHAIN.ETH: Web3(HTTPProvider(CHAIN_NODE_URLS[CHAIN.ETH])),
    CHAIN.POLYGON: Web3(HTTPProvider(CHAIN_NODE_URLS[CHAIN.POLYGON])),
    CHAIN.BSC_ARCHIVE: Web3(HTTPProvider(CHAIN_NODE_URLS[CHAIN.BSC_ARCHIVE])),
    CHAIN.ETH_ARCHIVE: Web3(HTTPProvider(CHAIN_NODE_URLS[CHAIN.ETH_ARCHIVE])),
    None: None
}

