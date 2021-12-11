

from typing import Union, Any
from dataclasses import dataclass


@dataclass
class ArbiLP:
    token1: str
    token2: str
    reserve1: float
    reserve2: float
    ratio: float
    logratio: float
    neglogratio: float
    lpAddress: str
    token1Address: str
    token2Address: str


@dataclass
class BFBacktracker:
    fromI: int
    lp: ArbiLP
    neglogratio: float



@dataclass
class ArbiOpportunity:
    tokenAddresses: [str]
    tokens: [str]
    lps: [ArbiLP]
    totalLogRatio: float
    totalGain: float



@dataclass
class OutputLP:
    token1: str
    token2: str
    # # reserve1: float
    # # reserve2: float
    # ratio: float
    # logratio: float
    # neglogratio: float
    lpAddress: str
    token1Address: str
    token2Address: str


@dataclass
class OutputAO:
    tokenAddresses: [str]
    tokens: [str]
    lps: [OutputLP]
    profit: float



@dataclass
class ArbiStep:
    tokenAddresses: str
    tokenSymbol: str
    lpAddress: str
    simAmount: float



