import json
import re
from typing import Any, Dict, Optional, Sequence

from ascii_library.orchestration.pipes import emr_constants
from boto3 import client


class CloudInstanceConfig:
    def __init__(self, **kwargs):
        self.instanceRole = kwargs.get("InstanceRole")
        self.market = kwargs.get("Market")
        self.name = kwargs.get("Name")
        self.instanceType = kwargs.get("InstanceType")
        self.workerInstanceType = kwargs.get(
            "WorkerInstanceType", kwargs.get("InstanceType")
        )

        self.bidPrice = kwargs.get("BidPrice")
        self.customAmiId = kwargs.get("CustomAmiId")
        self.sizeInGB = kwargs.get("SizeInGB", emr_constants.sizeInGB)
        self.volumeType = kwargs.get("VolumeType", emr_constants.volumeType)
        self.volumesPerInstance = kwargs.get(
            "VolumesPerInstance", emr_constants.volumesPerInstance
        )
        self.ebsOptimized = kwargs.get("EbsOptimized")
        self.instanceCount = kwargs.get("InstanceCount")
        self.masterInstanceCount = kwargs.get(
            "MasterInstanceCount", kwargs.get("InstanceCount", 1)
        )
        self.coreInstanceCount = kwargs.get(
            "CoreInstanceCount", kwargs.get("InstanceCount", 1)
        )
        self.taskInstanceCount = kwargs.get(
            "TaskInstanceCount", kwargs.get("InstanceCount", 1)
        )
        self.percentageOfOnDemandPrice = kwargs.get(
            "PercentageOfOnDemandPrice", emr_constants.percentageOfOnDemandPrice
        )
        self.reservationPreference = kwargs.get("ReservationPreference")
        self.allocationStrategy = kwargs.get("AllocationStrategy")
        self.timeoutAction = kwargs.get("TimeoutAction")
        self.timeoutDuration = kwargs.get(
            "TimeoutDuration", emr_constants.timeoutDuration
        )
        self.targetOnDemand = kwargs.get("TargetOnDemand")
        self.targetOnSpot = kwargs.get("TargetOnSpot")
        self.weightedCapacity = kwargs.get("WeightedCapacity")
        self.memoryGB = kwargs.get("MemoryGB")
        self.storageGB = kwargs.get("StorageGB")
        self.noEBS = kwargs.get("NoEBS", False)
        self.filters = kwargs.get("Filters")

    def create_filters(self, instance, ops: str = "Linux", region: str = "us-east-1"):
        """
        create an array with the filters fo price
        :param insance: json that from of an available instances on EMR,
        :param ops: operative system, EMR only takes Linux
        :param region: the region we work, by default it's us-east-1
        :return:
        """
        return [
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance["Type"]},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": ops},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {
                "Type": "TERM_MATCH",
                "Field": "licenseModel",
                "Value": "No License required",
            },
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
        ]

    def get_instance_price(self, prices):
        """
        parse json with prices for getting the price fo the right type
        :param prics: a list with the return of pricing client
        :return: float with the right price
        """
        for i in range(len(prices)):
            terms = json.loads(prices[i])["terms"]
            for sku, details in terms.items():
                if sku == "OnDemand":
                    priceDimension = details[list(details.keys())[0]]["priceDimensions"]
                    description = priceDimension[list(priceDimension.keys())[0]]
                    if (
                        description["unit"] == "Hrs"
                        and float(description["pricePerUnit"]["USD"]) > 0.0
                    ):
                        return float(description["pricePerUnit"]["USD"])

    def add_prefix_suffix_to_instances(self, instances):
        """
        Adds 'Prefix' and 'Suffix' to each instance dictionary based on the 'Type'.

        :param instances: List of instance dictionaries with a 'Type' key.
        :return: The original list of instances, modified to include 'Prefix' and 'Suffix'.
        """
        for instance in instances:
            type_match = re.split(r"\.\s*", instance["Type"])
            instance["Prefix"] = type_match[0]
            instance["Suffix"] = type_match[1]
        return instances

    def filter_by_numeric_value(self, instances, key, value):
        return [instance for instance in instances if instance.get(key, 0) >= value]

    def filter_by_exact_match(self, instances, key, value):
        return [instance for instance in instances if instance.get(key) == value]

    def filter_by_enum(
        self, instances, value, base=emr_constants.Suffix
    ):  # I think this method could be extended to other enums if needed
        target_index = base.index_of(value)
        if target_index != -1:  # Make sure the suffix provided is valid
            return [
                instance
                for instance in instances
                if base.index_of(instance.get("Suffix")) <= target_index
            ]

    def filter_instances(self, instances, **kwargs):
        """
        Filters instances based on given criteria. Numeric fields are compared for greater or equal values,
        while string fields must match exactly.

        :param instances: List of instance dictionaries.
        :param kwargs: Criteria for filtering (e.g., MemoryGB=16, VCPU=4, Prefix='m5').
        :return: List of filtered instances.
        """
        for key, value in kwargs.items():
            if isinstance(value, (int, float)) and key in [
                "MemoryGB",
                "VCPU",
                "StorageGB",
            ]:
                # Filter for numeric values (equal or greater)
                instances = self.filter_by_numeric_value(instances, key, value)
            elif isinstance(value, str) and key in ["InstanceFamilyId", "Prefix"]:
                # Filter for exact string match
                instances = self.filter_by_exact_match(instances, key, value)
            elif isinstance(value, str) and key == "Suffix":
                instances = self.filter_by_enum(instances=instances, value=value)
        return instances

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

    def _add_bid_price(self, config):
        if self.market == emr_constants.Market.spot and self.bidPrice is not None:
            config["BidPrice"] = self.bidPrice

    def _add_custom_ami_id(self, config, index: int = 0):
        if self.customAmiId[index] is not None:
            config["CustomAmiId"] = self.customAmiId[index]

    def _add_ebs_configuration(self, config):
        # if self._is_ebs_configuration_valid():
        ebsConfiguration = {"EbsOptimized": self.ebsOptimized}
        ebsBlockDeviceConfigs = self._build_ebs_block_device_config()
        ebsConfiguration["EbsBlockDeviceConfigs"] = ebsBlockDeviceConfigs
        config["EbsConfiguration"] = ebsConfiguration

    def _is_ebs_configuration_valid(self):
        return (
            len(self.sizeInGB) == len(self.volumeType) == len(self.volumesPerInstance)
        )

    def _build_ebs_block_device_config(self, index: int = 0):
        volume_size = (
            self.sizeInGB if isinstance(self.sizeInGB, int) else self.sizeInGB[index]
        )
        volume_type = (
            self.volumeType
            if isinstance(self.volumeType, emr_constants.VolumeType)
            else self.volumeType[index]
        )
        block = {
            "VolumeSpecification": {
                "SizeInGB": volume_size,
                "VolumeType": volume_type.value,
            }
        }
        block["VolumesPerInstance"] = self.volumesPerInstance[index]
        return [block]

    def gen_instance_type_config(self):
        instanceTypeConfigs = []
        ebsBlockDeviceConfigs = self._build_ebs_block_device_config()
        temp = {
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": ebsBlockDeviceConfigs[0],
                "EbsOptimized": self.ebsOptimized[0],
            },
            "InstanceType": self.instanceType[0],
            "WeightedCapacity": self.weightedCapacity[0],
        }
        if self.percentageOfOnDemandPrice is not None:
            temp["BidPriceAsPercentageOfOnDemandPrice"] = self.percentageOfOnDemandPrice
        if self.bidPrice[0] is not None:
            temp["BidPrice"] = self.bidPrice[0]
        instanceTypeConfigs.append(temp)
        return instanceTypeConfigs

    def get_launch_spec(self):
        launchSpecifications = {}
        if self.reservationPreference is not None:
            launchSpecifications["OnDemandSpecification"] = {
                "AllocationStrategy": emr_constants.AllocationStrategy.price.value,  # it's literally the only valid value, that's why it's hardcoded
                "CapacityReservationOptions": {
                    "CapacityReservationPreference": self.reservationPreference.value
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
        instanceTypeConfigs = self.gen_instance_type_configs()
        fleet_config = {"InstanceFleetType": self.instanceRole, "Name": self.name}

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

    def get_default_fleet(self, **kwargs):

        master_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.master.value,
            Market=emr_constants.Market.on_demand.value,
            Name="master",
            InstanceType=[self.instanceType],
            WeightedCapacity=[1],
            SizeInGB=[self.sizeInGB],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=[self.ebsOptimized],
            TargetOnDemand=1,
            ReservationPreference=self.reservationPreference,
            TimeoutDuration=self.timeoutDuration,
        )

        core_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.core.value,
            Market=emr_constants.Market.on_demand.value,
            Name="core",
            InstanceType=[kwargs.get("WorkerInstanceType", self.instanceType)],
            SizeInGB=[self.sizeInGB],
            WeightedCapacity=[self.weightedCapacity],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=[self.ebsOptimized],
            TargetOnDemand=self.targetOnDemand,
            ReservationPreference=self.reservationPreference,
            TimeoutDuration=self.timeoutDuration,
        )

        task_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.task.value,
            Market=emr_constants.Market.spot.value,
            Name="task",
            InstanceType=[kwargs.get("WorkerInstanceType", self.instanceType)],
            SizeInGB=[self.sizeInGB],
            WeightedCapacity=[self.weightedCapacity],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=[self.ebsOptimized],
            PercentageOfOnDemandPrice=self.percentageOfOnDemandPrice,
            AllocationStrategy=self.allocationStrategy,
            TimeoutAction=self.timeoutAction,
            ReservationPreference=self.reservationPreference,
            TimeoutDuration=self.timeoutDuration,
            TargetOnDemand=0,
            TargetOnSpot=self.targetOnSpot,
        )

        return [
            master_config.get_fleet_config(),
            core_config.get_fleet_config(),
            task_config.get_fleet_config(),
        ]

    def get_available_instances(
        self,
        emr_client: client,
        releaseLabel: Optional[str] = emr_constants.releaseLabel,
        **kwargs,
    ):
        result = []
        response: Dict[str, Any] = {}
        while True:
            if response.get("Marker") is None:
                response = emr_client.list_supported_instance_types(
                    ReleaseLabel=releaseLabel
                )
            else:
                response = emr_client.list_supported_instance_types(
                    ReleaseLabel=releaseLabel, Marker=response.get("Marker")
                )
            result.extend(
                self.add_prefix_suffix_to_instances(
                    response.get("SupportedInstanceTypes")
                )
            )
            if response.get("Marker") is None:
                break
        return result

    def get_instance_w_price(
        self, price_client: client, instances: Sequence[Dict], n: int = 15
    ):
        for instance in instances:
            response = price_client.get_products(
                ServiceCode="AmazonEC2",
                Filters=self.create_filters(instance),
                FormatVersion="aws_v1",
            )
            instance["Price"] = self.get_instance_price(response.get("PriceList"))
        return sorted(
            instances, key=lambda x: (x["Price"], -x["MemoryGB"], -x["VCPU"])
        )[0:n]

    def gen_instance_type_configs(self):
        instanceTypeConfigs = []
        for i in range(len(self.instanceType)):
            temp = {
                "InstanceType": self.instanceType[i],
                "WeightedCapacity": self.weightedCapacity[i],
            }
            if self.ebsOptimized[i] is True:
                ebsBlockDeviceConfigs = self._build_ebs_block_device_config(index=i)
                ebsConfiguration = {
                    "EbsBlockDeviceConfigs": ebsBlockDeviceConfigs,
                    "EbsOptimized": self.ebsOptimized[i],
                }
                temp["EbsConfiguration"] = ebsConfiguration
            if self.percentageOfOnDemandPrice is not None:
                temp["BidPriceAsPercentageOfOnDemandPrice"] = (
                    self.percentageOfOnDemandPrice
                )
            elif self.bidPrice is not None:
                temp["BidPrice"] = self.bidPrice
            instanceTypeConfigs.append(temp)
        return instanceTypeConfigs

    def get_fleet_programatically(self, emrClient: client, priceClient: client):
        filter_values = [
            self.filters.get("VCPU"),
            self.filters.get(
                "WorkerInstanceFamilyId", self.filters.get("InstanceFamilyId")
            ),
            self.filters.get("MemoryGB"),
            self.filters.get("WorkerPrefix", self.filters.get("Prefix")),
            self.filters.get("WorkerSuffix", self.filters.get("Suffix")),
        ]
        if self.filters.get("StorageGB") is None or all(
            value is None for value in filter_values
        ):
            raise
        instances = self.get_available_instances(emr_client=emrClient)
        master_candidates = self.filter_instances(
            instances,
            StorageGB=(
                self.filters.get("StorageGB")
                if self.filters.get("NoEBS") is True
                else None
            ),
            VCPU=self.filters.get("VCPU"),
            InstanceFamilyId=self.filters.get("InstanceFamilyId"),
            MemoryGB=self.filters.get("MemoryGB"),
            Prefix=self.filters.get("Prefix"),
            Suffix=self.filters.get("Suffix"),
        )
        master_candidates = self.get_instance_w_price(
            price_client=priceClient, instances=master_candidates, n=5
        )
        candidates = self.filter_instances(
            instances,
            StorageGB=(
                self.filters.get("StorageGB")
                if self.filters.get("NoEBS") is True
                else None
            ),
            VCPU=self.filters.get("VCPU"),
            InstanceFamilyId=self.filters.get(
                "WorkerInstanceFamilyId", self.filters.get("InstanceFamilyId")
            ),
            Prefix=self.filters.get("WorkerPrefix", self.filters.get("Prefix")),
            Suffix=self.filters.get("WorkerSuffix", self.filters.get("Suffix")),
            MemoryGB=self.filters.get("MemoryGB"),
        )
        candidates = self.get_instance_w_price(
            price_client=priceClient, instances=candidates
        )

        master_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.master.value,
            Market=emr_constants.Market.on_demand.value,
            Name="master",
            InstanceType=[item["Type"] for item in master_candidates],
            SizeInGB=[self.sizeInGB for _ in range(len(master_candidates))],
            WeightedCapacity=[1 for _ in range(len(master_candidates))],
            MemoryGB=[item["MemoryGB"] for item in master_candidates],
            StorageGB=[item["StorageGB"] for item in master_candidates],
            NoEBS=self.noEBS,
            VolumeType=[self.volumeType for _ in range(len(master_candidates))],
            VolumesPerInstance=[
                self.volumesPerInstance for _ in range(len(master_candidates))
            ],
            EbsOptimized=[
                (
                    item["EbsOptimizedAvailable"]
                    if item["StorageGB"] < self.filters.get("StorageGB")
                    else False
                )
                for item in master_candidates
            ],
            TargetOnDemand=1,
            ReservationPreference=self.reservationPreference,
            TimeoutDuration=self.timeoutDuration,
        )

        core_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.core.value,
            Market=emr_constants.Market.on_demand.value,
            Name="core",
            InstanceType=[item["Type"] for item in candidates[0:5]],
            SizeInGB=[self.sizeInGB for _ in range(len(candidates[0:5]))],
            WeightedCapacity=[
                (
                    self.weightedCapacity
                    if self.weightedCapacity is not None
                    else item["VCPU"]
                )
                for item in candidates[0:5]
            ],
            MemoryGB=[item["MemoryGB"] for item in candidates[0:5]],
            StorageGB=[item["StorageGB"] for item in candidates[0:5]],
            NoEBS=self.noEBS,
            VolumeType=[self.volumeType for _ in range(len(candidates[0:5]))],
            VolumesPerInstance=[
                self.volumesPerInstance for _ in range(len(candidates[0:5]))
            ],
            EbsOptimized=[
                (
                    item["EbsOptimizedAvailable"]
                    if item["StorageGB"] < self.filters.get("StorageGB")
                    else False
                )
                for item in candidates[0:5]
            ],
            TargetOnDemand=self.targetOnDemand,
            ReservationPreference=self.reservationPreference,
            TimeoutDuration=self.timeoutDuration,
        )
        task_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.task.value,
            Market=emr_constants.Market.spot.value,
            Name="task",
            InstanceType=[item["Type"] for item in candidates],
            SizeInGB=[self.sizeInGB for _ in range(len(candidates))],
            WeightedCapacity=[
                (
                    self.weightedCapacity
                    if self.weightedCapacity is not None
                    else item["VCPU"]
                )
                for item in candidates
            ],
            MemoryGB=[item["MemoryGB"] for item in candidates],
            StorageGB=[item["StorageGB"] for item in candidates],
            NoEBS=self.noEBS if self.noEBS is not None else False,
            VolumeType=[self.volumeType for _ in range(len(candidates))],
            VolumesPerInstance=[
                self.volumesPerInstance for _ in range(len(candidates))
            ],
            EbsOptimized=[
                (
                    item["EbsOptimizedAvailable"]
                    if item["StorageGB"] < self.filters.get("StorageGB")
                    else False
                )
                for item in candidates
            ],
            ReservationPreference=self.reservationPreference,
            PercentageOfOnDemandPrice=self.percentageOfOnDemandPrice,
            AllocationStrategy=(
                self.allocationStrategy
                if self.allocationStrategy is None
                else emr_constants.allocationStrategy
            ),
            TimeoutAction=self.timeoutAction,
            TimeoutDuration=self.timeoutDuration,
            TargetOnDemand=0,
            TargetOnSpot=self.targetOnSpot,
        )

        return [
            master_config.get_fleet_config(),
            core_config.get_fleet_config(),
            task_config.get_fleet_config(),
        ]

    def get_group_default(self):
        master_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.master,
            Market=emr_constants.Market.on_demand,
            Name="Master Group",
            CustomAmiId=[self.customAmiId],
            InstanceType=self.instanceType,
            InstanceCount=self.masterInstanceCount,
            BidPrice=self.bidPrice,
            SizeInGB=[self.sizeInGB],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=self.ebsOptimized if self.ebsOptimized is not None else True,
        )

        core_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.core,
            Market=emr_constants.Market.on_demand,
            Name="Core Group",
            CustomAmiId=[self.customAmiId],
            InstanceType=self.workerInstanceType,
            InstanceCount=self.coreInstanceCount,
            BidPrice=self.bidPrice,
            SizeInGB=[self.sizeInGB],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=self.ebsOptimized if self.ebsOptimized is not None else True,
        )

        task_config = CloudInstanceConfig(
            InstanceRole=emr_constants.InstanceRole.task,
            Market=emr_constants.Market.spot,
            Name="Task Group",
            CustomAmiId=[self.customAmiId],
            InstanceType=self.workerInstanceType,
            InstanceCount=self.taskInstanceCount,
            BidPrice=self.bidPrice,
            SizeInGB=[self.sizeInGB],
            VolumeType=[self.volumeType],
            VolumesPerInstance=[self.volumesPerInstance],
            EbsOptimized=self.ebsOptimized if self.ebsOptimized is not None else True,
        )
        return [
            master_config.get_instance_config(),
            core_config.get_instance_config(),
            task_config.get_instance_config(),
        ]
