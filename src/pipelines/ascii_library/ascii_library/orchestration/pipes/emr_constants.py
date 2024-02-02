from enum import Enum


class InstanceRole(Enum):
    master = "MASTER"
    core = "CORE"
    task = "TASK"


class TimeoutAction(Enum):
    switch = "SWITCH_TO_ON_DEMAND"
    terminate = "TERMINATE_CLUSTER"


# TODO: maybe we should make a list of valid instances because
#   a) not all of the instances work on EMR,
#   b) some are too big and we should not let the user pick a absurdly humongous instance


class Market(Enum):
    on_demand = "ON_DEMAND"
    spot = "SPOT"


class CapacityReservation(Enum):
    open = "open"
    close = "none"


class AllocationStrategy(Enum):
    priceCapacity = "price-capacity-optimized"
    capacity = "capacity-optimized"
    price = "lowest-price"
    mix = "diversified"


pipeline_bucket = "ascii-supply-chain-research-pipeline"
mock = False
sizeInGB = 100
volumeType = "gp2"
volumesPerInstance = 1
ebsOptimized = True
percentageOfOnDemandPrice = 70.0
allocationStrategy = AllocationStrategy.price
timeoutDuration = 60
weightedCapacity = (
    4  # this should be a number greater than 1 and should match the instance vcore
)
