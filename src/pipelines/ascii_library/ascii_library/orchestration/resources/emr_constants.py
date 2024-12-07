from enum import Enum, auto


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


class InstanceFamilyId(Enum):
    balanceCur = "GENERAL_CURRENT_GEN"
    balancePrev = "GENERAL_PREVIOUS_GEN"
    acceleratedComp = "GPU_CURRENT_GEN"
    memoryOptimizedCur = "HI_MEM_CURRENT_GEN"
    memoryOptimizedPrev = "HI_MEM_PREVIOUS_GEN"
    machineLearningCur = "COMPUTE_CURRENT_GEN"
    machineLearningPrev = "COMPUTE_PREVIOUS_GEN"
    IOPScur = "STORAGE_CURRENT_GEN"
    IOPSprev = "STORAGE_PREVIOUS_GEN"


class Suffix(Enum):
    small = auto()
    medium = auto()
    large = auto()
    xlarge = auto()
    _2xlarge = auto()
    _6xlarge = auto()
    _8xlarge = auto()
    _9xlarge = auto()
    _3xlarge = auto()
    _4xlarge = auto()
    _10xlarge = auto()
    _12xlarge = auto()
    _16xlarge = auto()
    _18xlarge = auto()
    _24xlarge = auto()
    _32xlarge = auto()
    _48xlarge = auto()

    @classmethod
    def suffix_order(cls):
        return list(cls)

    @classmethod
    def index_of(cls, suffix_str):
        # Adjust the suffix_str to match the enum naming convention that it cannot start with number
        # If the suffix_str starts with a digit, prepend an underscore
        if suffix_str[0].isdigit():
            suffix_str = f"_{suffix_str}"
        try:
            return cls.suffix_order().index(cls[suffix_str])
        except KeyError:
            return -1


class VolumeType(Enum):
    generalPurpose3 = "gp3"
    generalPurpose2 = "gp2"
    IOPS1 = "io1"
    throughputOptimizedHDD = "st1"
    coldHDD = "sc1"
    magnetic = "standard"


pipeline_bucket = "ascii-supply-chain-research-pipeline"
mock = False
sizeInGB = 300
volumeType = VolumeType.generalPurpose2
volumesPerInstance = 1
ebsOptimized = True
percentageOfOnDemandPrice = 70.0
allocationStrategy = AllocationStrategy.priceCapacity
timeoutDuration = 60
weightedCapacity = (
    4  # this should be a number greater than 1 and should match the instance vcore
)
releaseLabel = "emr-7.5.0"
