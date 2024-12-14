# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "kubernetes",
#     "pydantic",
#     "rich",
#     "tqdm",
# ]
# ///

from kubernetes import client, config
from tqdm import tqdm
from rich.console import Console
from rich.table import Table
import math
import re
from pydantic import BaseModel

# Pydantic models to validate metrics structure
class ResourceUsage(BaseModel):
    cpu: str
    memory: str

class NodeMetrics(BaseModel):
    apiVersion: str
    kind: str
    metadata: dict
    timestamp: str
    window: str
    usage: ResourceUsage

class ContainerUsage(BaseModel):
    name: str
    usage: ResourceUsage

class PodMetrics(BaseModel):
    apiVersion: str
    kind: str
    metadata: dict
    timestamp: str
    window: str
    containers: list[ContainerUsage]


class KubernetesMemoryAnalyzer:
    def __init__(self):
        # Load Kubernetes configuration
        config.load_kube_config()
        self.core_api = client.CoreV1Api()
        self.apps_api = client.AppsV1Api()
        self.metrics_api = client.CustomObjectsApi()
        self.console = Console()

    @staticmethod
    def parse_memory(value: str) -> int:
        """
        Parse memory usage strings like '6374M', '1024Ki', or '2G' into MiB.
        """
        units = {"Ki": 1 / 1024, "M": 1, "Mi": 1, "G": 1024, "Gi": 1024}
        match = re.match(r"^(\d+)([a-zA-Z]+)?$", value)
        if not match:
            raise ValueError(f"Invalid memory format: {value}")

        number = int(match.group(1))
        unit = match.group(2) or "M"  # Default to "M" (MiB) if no unit is provided

        if unit not in units:
            raise ValueError(f"Unsupported memory unit: {unit}")

        return int(number * units[unit])

    def fetch_pods_for_controller(
        self, 
        controller_kind: str, 
        controller_name: str, 
        namespace: str
    ) -> list[client.V1Pod]:
        """
        Fetch all pods managed by a given controller using its label selector.
        """
        if controller_kind in ["ReplicaSet", "StatefulSet", "DaemonSet"]:
            # Fetch the controller object to get its label selector
            if controller_kind == "ReplicaSet":
                controller = self.apps_api.read_namespaced_replica_set(controller_name, namespace)
            elif controller_kind == "StatefulSet":
                controller = self.apps_api.read_namespaced_stateful_set(controller_name, namespace)
            elif controller_kind == "DaemonSet":
                controller = self.apps_api.read_namespaced_daemon_set(controller_name, namespace)
            else:
                raise ValueError(f"Unsupported controller kind: {controller_kind}")

            # Extract the label selector
            label_selector = ",".join(
                [f"{key}={value}" for key, value in controller.spec.selector.match_labels.items()]
            )
            return self.core_api.list_namespaced_pod(namespace, label_selector=label_selector).items

        elif controller_kind == "Deployment":
            # Deployment pods are managed by a ReplicaSet, so find the ReplicaSet
            replicasets = self.apps_api.list_namespaced_replica_set(namespace).items
            for rs in replicasets:
                if rs.metadata.owner_references:
                    for owner in rs.metadata.owner_references:
                        if owner.kind == "Deployment" and owner.name == controller_name:
                            return self.fetch_pods_for_controller("ReplicaSet", rs.metadata.name, namespace)

        raise ValueError(f"Unsupported controller kind: {controller_kind}")

    @staticmethod
    def get_controller_kind_and_name(pod: client.V1Pod) -> tuple[str | None, str | None]:
        """
        Identify the higher-level controller managing the pod.
        """
        owner_references = pod.metadata.owner_references
        if owner_references:
            for owner in owner_references:
                return owner.kind, owner.name
        return None, None

    def get_memory_utilization(self, resource_type: str, namespace: str | None, name: str) -> int | None:
        """
        Fetch memory utilization for a given resource (node or pod).
        """
        try:
            if resource_type == "node":
                metrics = self.metrics_api.get_cluster_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    plural="nodes",
                    name=name
                )
                node_metrics = NodeMetrics(**metrics)
                return self.parse_memory(node_metrics.usage.memory)

            elif resource_type == "pod":
                metrics = self.metrics_api.get_namespaced_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    namespace=namespace,
                    plural="pods",
                    name=name
                )
                pod_metrics = PodMetrics(**metrics)
                return sum(self.parse_memory(container.usage.memory) for container in pod_metrics.containers)
            else:
                raise ValueError(f"Unsupported resource type: {resource_type}")

        except Exception as e:
            self.console.print(f"[bold red]Error fetching memory for {resource_type} {name}: {e}[/bold red]")
            return None

    def analyze_nodes(self) -> list[str]:
        """
        Analyze memory utilization for all nodes.
        """
        nodes = self.core_api.list_node().items
        node_table = Table(title="Kubernetes Node Memory Utilization")
        node_table.add_column("Node Name", style="bold cyan")
        node_table.add_column("Memory Capacity (Mi)", justify="right")
        node_table.add_column("Memory Usage (Mi)", justify="right")
        node_table.add_column("Utilization (%)", justify="right", style="bold magenta")
        node_table.add_column("Status", style="bold red")

        high_usage_nodes = []

        for node in tqdm(nodes, desc="Analyzing node memory utilization"):
            node_name = node.metadata.name
            memory_capacity = int(node.status.capacity["memory"].rstrip("Ki")) / 1024
            used_memory = self.get_memory_utilization("node", None, node_name)

            if used_memory is None:
                continue

            utilization_percentage = (used_memory / memory_capacity) * 100
            status = "⚠️ High Usage" if utilization_percentage > 80 else "OK"

            if utilization_percentage > 80:
                high_usage_nodes.append(node_name)

            node_table.add_row(
                node_name,
                f"{math.ceil(memory_capacity):,}",
                f"{math.ceil(used_memory):,}",
                f"{utilization_percentage:.2f}",
                status
            )

        self.console.print(node_table)
        return high_usage_nodes

    def analyze_pods_on_node(self, node_name: str) -> client.V1Pod | None:
        """
        Analyze memory utilization for all pods on a specific node.
        """
        pods = self.core_api.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}").items
        pod_table = Table(title=f"Pods Memory Utilization on Node: {node_name}", show_lines=True)
        pod_table.add_column("Pod Name", style="bold cyan")
        pod_table.add_column("Namespace", style="bold green")
        pod_table.add_column("Memory Usage (Mi)", justify="right", style="bold magenta")

        highest_usage_pod = None
        highest_usage = 0

        for pod in tqdm(pods, desc=f"Analyzing pods on {node_name}"):
            pod_name = pod.metadata.name
            namespace = pod.metadata.namespace
            pod_memory = self.get_memory_utilization("pod", namespace, pod_name)

            if pod_memory is None:
                continue

            if pod_memory > highest_usage:
                highest_usage = pod_memory
                highest_usage_pod = pod

            pod_table.add_row(pod_name, namespace, f"{math.ceil(pod_memory):,}")

        self.console.print(pod_table)
        return highest_usage_pod

    def analyze_controller(self, controller_kind: str, controller_name: str, namespace: str):
        """
        Analyze memory utilization for all pods managed by a controller.
        """
        try:
            controller_pods = self.fetch_pods_for_controller(controller_kind, controller_name, namespace)
            controller_table = Table(title=f"Memory Utilization for {controller_kind} {controller_name}", show_lines=True)
            controller_table.add_column("Pod Name", style="bold cyan")
            controller_table.add_column("Node Name", style="bold green")
            controller_table.add_column("Memory Usage (Mi)", justify="right", style="bold magenta")

            for pod in tqdm(controller_pods, desc=f"Analyzing pods for {controller_kind} {controller_name}"):
                pod_name = pod.metadata.name
                pod_node = pod.spec.node_name
                pod_memory = self.get_memory_utilization("pod", pod.metadata.namespace, pod_name)

                if pod_memory is None:
                    continue

                controller_table.add_row(pod_name, pod_node, f"{math.ceil(pod_memory):,}")

            self.console.print(controller_table)
        except Exception as e:
            self.console.print(f"[bold red]Error analyzing {controller_kind} {controller_name}: {e}[/bold red]")

    def run(self):
        """
        Main execution loop.
        """
        high_usage_nodes = self.analyze_nodes()

        for node_name in high_usage_nodes:
            highest_usage_pod = self.analyze_pods_on_node(node_name)

            if highest_usage_pod:
                controller_kind, controller_name = self.get_controller_kind_and_name(highest_usage_pod)
                if controller_kind and controller_name:
                    self.analyze_controller(controller_kind, controller_name, highest_usage_pod.metadata.namespace)


if __name__ == "__main__":
    analyzer = KubernetesMemoryAnalyzer()
    analyzer.run()
