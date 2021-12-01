
from nodeRegistry import *

def deprefixAddress(address):
    return address.split(':')[1] if ':' in address else address

def getChainFromPrefixedAddress(address):
    if address.lower().startswith(CHAIN.BSC.lower()):
        return CHAIN.BSC
    if address.lower().startswith(CHAIN.ETH.lower()):
        return CHAIN.ETH
    if address.lower().startswith(CHAIN.POLYGON.lower()):
        return CHAIN.POLYGON
    return None


# here we assume that the address is in the format "{chain:address}"
def getW3FromAddress(address):
    return W3S[getChainFromPrefixedAddress(address)]

def getArchiveW3FromAddress(address):
    return W3S[ARCHIVE_VERSION[getChainFromPrefixedAddress(address)]]

