from ascii_library.orchestration.pipes import emr_constants


class CloudInstanceConfig:
    def __init__(self, **kwargs):
        self.instanceRole = kwargs.get("instanceRole")
        self.market = kwargs.get("market")
        self.name = kwargs.get("name")
        self.instanceType = kwargs.get("instanceType")
        self.bidPrice = kwargs.get("bidPrice")
        self.customAmiId = kwargs.get("customAmiId")
        self.sizeInGB = kwargs.get("sizeInGB")
        self.volumeType = kwargs.get("volumeType")
        self.volumesPerInstance = kwargs.get("volumesPerInstance")
        self.ebsOptimized = kwargs.get("ebsOptimized")
        self.instanceCount = kwargs.get("instanceCount")
        self.percentageOfOnDemandPrice = kwargs.get("percentageOfOnDemandPrice")
        self.reservationPreference = kwargs.get("reservationPreference")
        self.allocationStrategy = kwargs.get("allocationStrategy")
        self.timeoutAction = kwargs.get("timeoutAction")
        self.timeoutDuration = kwargs.get("timeoutDuration")
        self.targetOnDemand = kwargs.get("targetOnDemand")
        self.targetOnSpot = kwargs.get("targetOnSpot")
        self.weightedCapacity = kwargs.get("weightedCapacity")

    def get_instance_config(self):
        config = self._base_config()
        self._add_bid_price(config)
        self._add_custom_ami_id(config)
        self._add_ebs_configuration(config)
        return config

    def _base_config(self):
        return {
            "InstanceCount": self.instanceCount,
            "InstanceRole": self.instanceRole.value,
            "InstanceType": self.instanceType,
            "Market": self.market.value,
            "Name": self.name,
        }

    def _add_bid_price(self, config, index: int = 0):
        if (
            self.market == emr_constants.Market.spot
            and self.bidPrice[index] is not None
        ):
            config["BidPrice"] = self.bidPrice[index]

    def _add_custom_ami_id(self, config, index: int = 0):
        if self.customAmiId[index] is not None:
            config["CustomAmiId"] = self.customAmiId[index]

    def _add_ebs_configuration(self, config):
        if self._is_ebs_configuration_valid():
            ebsConfiguration = {"EbsOptimized": self.ebsOptimized}
            ebsBlockDeviceConfigs = self._build_ebs_block_device_configs()
            ebsConfiguration["EbsBlockDeviceConfigs"] = ebsBlockDeviceConfigs
            config["EbsConfiguration"] = ebsConfiguration

    def _is_ebs_configuration_valid(self):
        return (
            len(self.sizeInGB) == len(self.volumeType) == len(self.volumesPerInstance)
        )

    def _build_ebs_block_device_configs(self):
        block = {
            "VolumeSpecification": {
                "SizeInGB": self.sizeInGB,
                "VolumeType": self.volumeType,
            }
        }
        if self.volumesPerInstance is not None:
            block["VolumesPerInstance"] = self.volumesPerInstance
        return [block]

    def gen_instance_type_config(self):
        instanceTypeConfigs = []
        ebsBlockDeviceConfigs = self._build_ebs_block_device_configs()
        temp = {
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": ebsBlockDeviceConfigs,
                "EbsOptimized": self.ebsOptimized,
            },
            "InstanceType": self.instanceType,
            "WeightedCapacity": self.weightedCapacity,
        }
        if self.percentageOfOnDemandPrice is not None:
            temp["BidPriceAsPercentageOfOnDemandPrice"] = self.percentageOfOnDemandPrice
        if self.bidPrice is not None:
            temp["BidPrice"] = self.bidPrice
        instanceTypeConfigs.append(temp)
        return instanceTypeConfigs

    def get_launch_spec(self):
        launchSpecifications = {}
        if self.reservationPreference is not None:
            launchSpecifications["OnDemandSpecification"] = {
                "AllocationStrategy": emr_constants.AllocationStrategy.price.value,  # it's literally the only valid value, that's why it's hardcoded
                "CapacityReservationOptions": {
                    "CapacityReservationPreference": self.reservationPreference
                },
            }
        if self.allocationStrategy is not None:
            launchSpecifications["SpotSpecification"] = {
                "AllocationStrategy": self.allocationStrategy.value,
                "TimeoutAction": self.timeoutAction.value,
                "TimeoutDurationMinutes": self.timeoutDuration,
            }
        return launchSpecifications

    def get_fleet_config(self):
        instanceTypeConfigs = self.gen_instance_type_config()
        fleet_config = {
            "InstanceFleetType": self.instanceRole,
            "Name": self.name,
        }

        if self.targetOnDemand is not None:
            fleet_config["TargetOnDemandCapacity"] = self.targetOnDemand
        if self.targetOnSpot is not None:
            fleet_config["TargetSpotCapacity"] = self.targetOnSpot
        fleet_config["InstanceTypeConfigs"] = instanceTypeConfigs
        if self.timeoutDuration is not None:
            fleet_config["ResizeSpecifications"] = {
                "OnDemandResizeSpecification": {
                    "TimeoutDurationMinutes": self.timeoutDuration
                },
                "SpotResizeSpecification": {
                    "TimeoutDurationMinutes": self.timeoutDuration
                },
            }
        fleet_config["LaunchSpecifications"] = self.get_launch_spec()
        return fleet_config

    @classmethod
    def get_default_fleet(
        cls,
        targetOnSpot,
        targetOnDemand,
        reservationPreference,
        timeoutAction,
        instanceType,
        **kwargs
    ):

        master_config = cls(
            instanceRole=emr_constants.InstanceRole.master.value,
            market=emr_constants.Market.on_demand.value,
            name="master",
            instanceType=kwargs.get("masterInstanceType", instanceType),
            weightedCapacity=1,
            sizeInGB=kwargs.get("sizeInGB", emr_constants.sizeInGB),
            volumeType=kwargs.get("volumeType", emr_constants.volumeType),
            volumesPerInstance=kwargs.get("volumesPerInstance"),
            ebsOptimized=kwargs.get("ebsOptimized", emr_constants.ebsOptimized),
            targetOnDemand=targetOnDemand,
            reservationPreference=reservationPreference.value,
            timeoutDuration=kwargs.get(
                "timeoutDuration", emr_constants.timeoutDuration
            ),
        )

        core_config = cls(
            instanceRole=emr_constants.InstanceRole.core.value,
            market=emr_constants.Market.on_demand.value,
            name="core",
            instanceType=instanceType,
            sizeInGB=kwargs.get("sizeInGB", emr_constants.sizeInGB),
            weightedCapacity=kwargs.get(
                "weightedCapacity", emr_constants.weightedCapacity
            ),
            volumeType=kwargs.get("volumeType", emr_constants.volumeType),
            volumesPerInstance=kwargs.get("volumesPerInstance"),
            ebsOptimized=kwargs.get("ebsOptimized", emr_constants.ebsOptimized),
            targetOnDemand=targetOnDemand,
            reservationPreference=reservationPreference.value,
            timeoutDuration=kwargs.get(
                "timeoutDuration", emr_constants.timeoutDuration
            ),
        )

        task_config = cls(
            instanceRole=emr_constants.InstanceRole.task.value,
            market=emr_constants.Market.spot.value,
            name="task",
            instanceType=instanceType,
            sizeInGB=kwargs.get("sizeInGB", emr_constants.sizeInGB),
            weightedCapacity=kwargs.get(
                "weightedCapacity", emr_constants.weightedCapacity
            ),
            volumeType=kwargs.get("volumeType", emr_constants.volumeType),
            volumesPerInstance=kwargs.get("volumesPerInstance"),
            ebsOptimized=kwargs.get("ebsOptimized", emr_constants.ebsOptimized),
            percentageOfOnDemandPrice=kwargs.get(
                "percentageOfOnDemandPrice", emr_constants.percentageOfOnDemandPrice
            ),
            allocationStrategy=kwargs.get(
                "allocationStrategy", emr_constants.allocationStrategy
            ),
            timeoutAction=timeoutAction,
            timeoutDuration=kwargs.get(
                "timeoutDuration", emr_constants.timeoutDuration
            ),
            targetOnDemand=0,
            targetOnSpot=targetOnSpot,
        )

        return [
            master_config.get_fleet_config(),
            core_config.get_fleet_config(),
            task_config.get_fleet_config(),
        ]
