/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package demo

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// Google infrastructure demo data generators
// Models: region -> zone -> cluster -> rack -> machine (physical)
//         cell -> job -> task -> alloc (workload)

// Region data
var googleRegions = []struct {
	region    string
	name      string
	continent string
}{
	{"us-east", "US East", "Americas"},
	{"us-west", "US West", "Americas"},
	{"us-central", "US Central", "Americas"},
	{"europe-west", "Europe West", "Europe"},
	{"europe-north", "Europe North", "Europe"},
	{"asia-east", "Asia East", "Asia-Pacific"},
	{"asia-south", "Asia South", "Asia-Pacific"},
	{"asia-northeast", "Asia Northeast", "Asia-Pacific"},
}

// CreateGoogleRegionsTable creates the regions table
func CreateGoogleRegionsTable() *tables.DataTable {
	t := tables.NewDataTable()

	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "google.region"))
	nameCol := columns.NewStringColumn(columns.NewColumnDef("name", "Name", ""))
	continentCol := columns.NewStringColumn(columns.NewColumnDef("continent", "Continent", ""))
	zoneCountCol := columns.NewUint32Column(columns.NewColumnDef("zone_count", "Zones", ""))
	totalMachinesCol := columns.NewUint32Column(columns.NewColumnDef("total_machines", "Total Machines", ""))
	networkCapacityCol := columns.NewFloat64Column(columns.NewColumnDef("network_capacity_tbps", "Network (Tbps)", ""))

	t.AddColumn(regionCol)
	t.AddColumn(nameCol)
	t.AddColumn(continentCol)
	t.AddColumn(zoneCountCol)
	t.AddColumn(totalMachinesCol)
	t.AddColumn(networkCapacityCol)

	for _, r := range googleRegions {
		regionCol.Append(r.region)
		nameCol.Append(r.name)
		continentCol.Append(r.continent)
		zoneCountCol.Append(3)                          // 3 zones per region
		totalMachinesCol.Append(uint32(3 * 5 * 20 * 5)) // zones * clusters * racks * machines
		networkCapacityCol.Append(10.0 + rand.Float64()*10.0)
	}

	// Finalize columns to detect uniqueness and enable joins
	regionCol.FinalizeColumn()
	nameCol.FinalizeColumn()
	continentCol.FinalizeColumn()
	zoneCountCol.FinalizeColumn()
	totalMachinesCol.FinalizeColumn()
	networkCapacityCol.FinalizeColumn()

	return t
}

// CreateGoogleZonesTable creates the zones table
func CreateGoogleZonesTable() *tables.DataTable {
	t := tables.NewDataTable()

	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "google.region"))
	nameCol := columns.NewStringColumn(columns.NewColumnDef("name", "Name", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	clusterCountCol := columns.NewUint32Column(columns.NewColumnDef("cluster_count", "Clusters", ""))
	totalMachinesCol := columns.NewUint32Column(columns.NewColumnDef("total_machines", "Total Machines", ""))
	powerCapacityCol := columns.NewFloat64Column(columns.NewColumnDef("power_capacity_mw", "Power (MW)", ""))
	pueCol := columns.NewFloat64Column(columns.NewColumnDef("pue", "PUE", ""))

	t.AddColumn(zoneCol)
	t.AddColumn(regionCol)
	t.AddColumn(nameCol)
	t.AddColumn(statusCol)
	t.AddColumn(clusterCountCol)
	t.AddColumn(totalMachinesCol)
	t.AddColumn(powerCapacityCol)
	t.AddColumn(pueCol)

	zoneStatuses := []string{"operational", "operational", "operational", "operational", "degraded", "maintenance"}

	for _, r := range googleRegions {
		for _, suffix := range []string{"a", "b", "c"} {
			zoneCol.Append(fmt.Sprintf("%s-%s", r.region, suffix))
			regionCol.Append(r.region)
			nameCol.Append(fmt.Sprintf("%s Zone %s", r.name, suffix))
			statusCol.Append(zoneStatuses[rand.Intn(len(zoneStatuses))])
			clusterCountCol.Append(5)
			totalMachinesCol.Append(uint32(5 * 20 * 5))
			powerCapacityCol.Append(50.0 + rand.Float64()*50.0)
			pueCol.Append(1.1 + rand.Float64()*0.2)
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	zoneCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	nameCol.FinalizeColumn()
	statusCol.FinalizeColumn()
	clusterCountCol.FinalizeColumn()
	totalMachinesCol.FinalizeColumn()
	powerCapacityCol.FinalizeColumn()
	pueCol.FinalizeColumn()

	return t
}

// CreateGoogleClustersTable creates the clusters table
func CreateGoogleClustersTable() *tables.DataTable {
	t := tables.NewDataTable()

	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "google.region"))
	purposeCol := columns.NewStringColumn(columns.NewColumnDef("purpose", "Purpose", ""))
	generationCol := columns.NewStringColumn(columns.NewColumnDef("generation", "Generation", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	rackCountCol := columns.NewUint32Column(columns.NewColumnDef("rack_count", "Racks", ""))
	totalMachinesCol := columns.NewUint32Column(columns.NewColumnDef("total_machines", "Machines", ""))

	t.AddColumn(clusterCol)
	t.AddColumn(zoneCol)
	t.AddColumn(regionCol)
	t.AddColumn(purposeCol)
	t.AddColumn(generationCol)
	t.AddColumn(statusCol)
	t.AddColumn(rackCountCol)
	t.AddColumn(totalMachinesCol)

	clusterPurposes := []string{"serving", "serving", "batch", "storage", "ml"}
	clusterGenerations := []string{"gen3", "gen4", "gen4", "gen5", "gen5"}
	clusterStatuses := []string{"active", "active", "active", "active", "draining", "offline"}

	for _, r := range googleRegions {
		for _, suffix := range []string{"a", "b", "c"} {
			zone := fmt.Sprintf("%s-%s", r.region, suffix)
			for i := 0; i < 5; i++ {
				clusterCol.Append(fmt.Sprintf("%s-c%d", zone, i))
				zoneCol.Append(zone)
				regionCol.Append(r.region)
				purposeCol.Append(clusterPurposes[i])
				generationCol.Append(clusterGenerations[rand.Intn(len(clusterGenerations))])
				statusCol.Append(clusterStatuses[rand.Intn(len(clusterStatuses))])
				rackCountCol.Append(20)
				totalMachinesCol.Append(uint32(20 * 5))
			}
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	clusterCol.FinalizeColumn()
	zoneCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	purposeCol.FinalizeColumn()
	generationCol.FinalizeColumn()
	statusCol.FinalizeColumn()
	rackCountCol.FinalizeColumn()
	totalMachinesCol.FinalizeColumn()

	return t
}

// clusterInfo holds cluster info for generating racks/machines
type clusterInfo struct {
	cluster string
	zone    string
	region  string
}

func getClusterList() []clusterInfo {
	var result []clusterInfo
	for _, r := range googleRegions {
		for _, suffix := range []string{"a", "b", "c"} {
			zone := fmt.Sprintf("%s-%s", r.region, suffix)
			for i := 0; i < 5; i++ {
				cluster := fmt.Sprintf("%s-c%d", zone, i)
				result = append(result, clusterInfo{cluster, zone, r.region})
			}
		}
	}
	return result
}

// CreateGoogleRacksTable creates the racks table
func CreateGoogleRacksTable() *tables.DataTable {
	t := tables.NewDataTable()

	rackCol := columns.NewStringColumn(columns.NewColumnDef("rack", "Rack", "google.rack"))
	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	positionCol := columns.NewStringColumn(columns.NewColumnDef("position", "Position", ""))
	machineCountCol := columns.NewUint32Column(columns.NewColumnDef("machine_count", "Machines", ""))
	powerDrawCol := columns.NewFloat64Column(columns.NewColumnDef("power_draw_kw", "Power (kW)", ""))
	networkSwitchCol := columns.NewStringColumn(columns.NewColumnDef("network_switch", "Switch", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))

	t.AddColumn(rackCol)
	t.AddColumn(clusterCol)
	t.AddColumn(zoneCol)
	t.AddColumn(positionCol)
	t.AddColumn(machineCountCol)
	t.AddColumn(powerDrawCol)
	t.AddColumn(networkSwitchCol)
	t.AddColumn(statusCol)

	rackStatuses := []string{"healthy", "healthy", "healthy", "healthy", "degraded", "offline"}
	rows := []string{"A", "B", "C", "D"}

	for _, c := range getClusterList() {
		for i := 0; i < 20; i++ {
			rackCol.Append(fmt.Sprintf("%s-r%02d", c.cluster, i))
			clusterCol.Append(c.cluster)
			zoneCol.Append(c.zone)
			positionCol.Append(fmt.Sprintf("%s-%02d", rows[i/5], i%5))
			machineCountCol.Append(uint32(4 + rand.Intn(3))) // 4-6 machines per rack
			powerDrawCol.Append(15.0 + rand.Float64()*10.0)
			networkSwitchCol.Append(fmt.Sprintf("sw-%s-%02d", c.cluster, i))
			statusCol.Append(rackStatuses[rand.Intn(len(rackStatuses))])
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	rackCol.FinalizeColumn()
	clusterCol.FinalizeColumn()
	zoneCol.FinalizeColumn()
	positionCol.FinalizeColumn()
	machineCountCol.FinalizeColumn()
	powerDrawCol.FinalizeColumn()
	networkSwitchCol.FinalizeColumn()
	statusCol.FinalizeColumn()

	return t
}

// rackInfo holds rack info for generating machines
type rackInfo struct {
	rack    string
	cluster string
	zone    string
}

func getRackList() []rackInfo {
	var result []rackInfo
	for _, c := range getClusterList() {
		for i := 0; i < 20; i++ {
			rack := fmt.Sprintf("%s-r%02d", c.cluster, i)
			result = append(result, rackInfo{rack, c.cluster, c.zone})
		}
	}
	return result
}

// CreateGoogleMachinesTable creates the machines table
func CreateGoogleMachinesTable() *tables.DataTable {
	t := tables.NewDataTable()

	machineCol := columns.NewStringColumn(columns.NewColumnDef("machine", "Machine", "google.machine"))
	rackCol := columns.NewStringColumn(columns.NewColumnDef("rack", "Rack", "google.rack"))
	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	cpuCoresCol := columns.NewUint32Column(columns.NewColumnDef("cpu_cores", "CPU Cores", ""))
	memoryGBCol := columns.NewUint32Column(columns.NewColumnDef("memory_gb", "Memory (GB)", ""))
	diskTBCol := columns.NewFloat64Column(columns.NewColumnDef("disk_tb", "Disk (TB)", ""))
	gpuCountCol := columns.NewUint32Column(columns.NewColumnDef("gpu_count", "GPUs", ""))
	cpuArchCol := columns.NewStringColumn(columns.NewColumnDef("cpu_arch", "Architecture", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	cpuFreeCoresCol := columns.NewFloat64Column(columns.NewColumnDef("cpu_free_cores", "Free CPU", ""))
	memoryFreeGBCol := columns.NewFloat64Column(columns.NewColumnDef("memory_free_gb", "Free Memory (GB)", ""))
	uptimeDaysCol := columns.NewUint32Column(columns.NewColumnDef("uptime_days", "Uptime (days)", ""))

	t.AddColumn(machineCol)
	t.AddColumn(rackCol)
	t.AddColumn(clusterCol)
	t.AddColumn(zoneCol)
	t.AddColumn(cpuCoresCol)
	t.AddColumn(memoryGBCol)
	t.AddColumn(diskTBCol)
	t.AddColumn(gpuCountCol)
	t.AddColumn(cpuArchCol)
	t.AddColumn(statusCol)
	t.AddColumn(cpuFreeCoresCol)
	t.AddColumn(memoryFreeGBCol)
	t.AddColumn(uptimeDaysCol)

	machineStatuses := []string{"healthy", "healthy", "healthy", "healthy", "healthy", "degraded", "dead", "repair"}
	cpuArchitecture := []string{"x86_64", "x86_64", "x86_64", "arm64"}

	for _, r := range getRackList() {
		machinesInRack := 4 + rand.Intn(3) // 4-6 machines per rack
		for i := 0; i < machinesInRack; i++ {
			machineCol.Append(fmt.Sprintf("%s-m%02d", r.rack, i))
			rackCol.Append(r.rack)
			clusterCol.Append(r.cluster)
			zoneCol.Append(r.zone)

			cores := uint32(64 + rand.Intn(64)) // 64-128 cores
			mem := uint32(256 + rand.Intn(512)) // 256-768 GB
			cpuCoresCol.Append(cores)
			memoryGBCol.Append(mem)
			diskTBCol.Append(2.0 + rand.Float64()*6.0)
			gpuCountCol.Append(uint32(rand.Intn(5))) // 0-4 GPUs
			cpuArchCol.Append(cpuArchitecture[rand.Intn(len(cpuArchitecture))])
			statusCol.Append(machineStatuses[rand.Intn(len(machineStatuses))])

			// Usage (some machines heavily used, others light)
			usagePct := 0.3 + rand.Float64()*0.6
			cpuFreeCoresCol.Append(float64(cores) * (1 - usagePct))
			memoryFreeGBCol.Append(float64(mem) * (1 - usagePct))
			uptimeDaysCol.Append(uint32(rand.Intn(365)))
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	machineCol.FinalizeColumn()
	rackCol.FinalizeColumn()
	clusterCol.FinalizeColumn()
	zoneCol.FinalizeColumn()
	cpuCoresCol.FinalizeColumn()
	memoryGBCol.FinalizeColumn()
	diskTBCol.FinalizeColumn()
	gpuCountCol.FinalizeColumn()
	cpuArchCol.FinalizeColumn()
	statusCol.FinalizeColumn()
	cpuFreeCoresCol.FinalizeColumn()
	memoryFreeGBCol.FinalizeColumn()
	uptimeDaysCol.FinalizeColumn()

	return t
}

// CreateGoogleCellsTable creates the cells table (1:1 with clusters)
func CreateGoogleCellsTable() *tables.DataTable {
	t := tables.NewDataTable()

	cellCol := columns.NewStringColumn(columns.NewColumnDef("cell", "Cell", "google.cell"))
	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	versionCol := columns.NewStringColumn(columns.NewColumnDef("version", "Version", ""))
	jobCountCol := columns.NewUint32Column(columns.NewColumnDef("job_count", "Jobs", ""))
	taskCountCol := columns.NewUint32Column(columns.NewColumnDef("task_count", "Tasks", ""))
	cpuAllocatedCol := columns.NewFloat64Column(columns.NewColumnDef("cpu_allocated", "CPU Allocated", ""))
	memoryAllocatedCol := columns.NewFloat64Column(columns.NewColumnDef("memory_allocated_gb", "Memory Allocated (GB)", ""))
	utilizationCol := columns.NewFloat64Column(columns.NewColumnDef("utilization_pct", "Utilization %", ""))

	t.AddColumn(cellCol)
	t.AddColumn(clusterCol)
	t.AddColumn(zoneCol)
	t.AddColumn(versionCol)
	t.AddColumn(jobCountCol)
	t.AddColumn(taskCountCol)
	t.AddColumn(cpuAllocatedCol)
	t.AddColumn(memoryAllocatedCol)
	t.AddColumn(utilizationCol)

	borgVersions := []string{"borg-v3.1", "borg-v3.2", "borg-v4.0", "borg-v4.1"}

	for _, c := range getClusterList() {
		cellCol.Append(fmt.Sprintf("cell-%s", c.cluster))
		clusterCol.Append(c.cluster)
		zoneCol.Append(c.zone)
		versionCol.Append(borgVersions[rand.Intn(len(borgVersions))])
		jobCountCol.Append(uint32(30 + rand.Intn(50)))
		taskCountCol.Append(uint32(300 + rand.Intn(500)))
		cpuAllocatedCol.Append(2000.0 + rand.Float64()*3000.0)
		memoryAllocatedCol.Append(8000.0 + rand.Float64()*12000.0)
		utilizationCol.Append(60.0 + rand.Float64()*30.0)
	}

	// Finalize columns to detect uniqueness and enable joins
	cellCol.FinalizeColumn()
	clusterCol.FinalizeColumn()
	zoneCol.FinalizeColumn()
	versionCol.FinalizeColumn()
	jobCountCol.FinalizeColumn()
	taskCountCol.FinalizeColumn()
	cpuAllocatedCol.FinalizeColumn()
	memoryAllocatedCol.FinalizeColumn()
	utilizationCol.FinalizeColumn()

	return t
}

// cellInfo holds cell info for generating jobs
type cellInfo struct {
	cell    string
	cluster string
	zone    string
}

func getCellList() []cellInfo {
	var result []cellInfo
	for _, c := range getClusterList() {
		cell := fmt.Sprintf("cell-%s", c.cluster)
		result = append(result, cellInfo{cell, c.cluster, c.zone})
	}
	return result
}

// CreateGoogleJobsTable creates the jobs table
func CreateGoogleJobsTable() *tables.DataTable {
	t := tables.NewDataTable()

	jobCol := columns.NewStringColumn(columns.NewColumnDef("job", "Job", "google.job"))
	cellCol := columns.NewStringColumn(columns.NewColumnDef("cell", "Cell", "google.cell"))
	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	zoneCol := columns.NewStringColumn(columns.NewColumnDef("zone", "Zone", "google.zone"))
	ownerCol := columns.NewStringColumn(columns.NewColumnDef("owner", "Owner", ""))
	priorityCol := columns.NewStringColumn(columns.NewColumnDef("priority", "Priority", ""))
	stateCol := columns.NewStringColumn(columns.NewColumnDef("state", "State", ""))
	taskCountCol := columns.NewUint32Column(columns.NewColumnDef("task_count", "Tasks", ""))
	cpuRequestCol := columns.NewFloat64Column(columns.NewColumnDef("cpu_request", "CPU Request", ""))
	memoryRequestCol := columns.NewFloat64Column(columns.NewColumnDef("memory_request_gb", "Memory Request (GB)", ""))
	createdAtCol := columns.NewDatetimeColumn(columns.NewColumnDef("created_at", "Created", ""))
	binaryCol := columns.NewStringColumn(columns.NewColumnDef("binary", "Binary", ""))

	t.AddColumn(jobCol)
	t.AddColumn(cellCol)
	t.AddColumn(clusterCol)
	t.AddColumn(zoneCol)
	t.AddColumn(ownerCol)
	t.AddColumn(priorityCol)
	t.AddColumn(stateCol)
	t.AddColumn(taskCountCol)
	t.AddColumn(cpuRequestCol)
	t.AddColumn(memoryRequestCol)
	t.AddColumn(createdAtCol)
	t.AddColumn(binaryCol)

	teams := []string{"search", "ads", "cloud", "youtube", "maps", "infra", "ml", "storage"}
	jobPriorities := []string{"production", "production", "production", "batch", "batch", "best-effort"}
	jobStates := []string{"running", "running", "running", "running", "pending", "stopped"}
	binaryNames := []string{"frontend", "backend", "worker", "indexer", "aggregator", "cache", "proxy", "scheduler"}

	cellList := getCellList()
	now := time.Now()

	// Generate ~40 jobs per cell = ~4800 jobs
	for _, c := range cellList {
		numJobs := 35 + rand.Intn(15)
		for i := 0; i < numJobs; i++ {
			owner := teams[rand.Intn(len(teams))]
			binary := binaryNames[rand.Intn(len(binaryNames))]
			jobCol.Append(fmt.Sprintf("%s/%s-%d", owner, binary, rand.Intn(1000)))
			cellCol.Append(c.cell)
			clusterCol.Append(c.cluster)
			zoneCol.Append(c.zone)
			ownerCol.Append(owner)
			priorityCol.Append(jobPriorities[rand.Intn(len(jobPriorities))])
			stateCol.Append(jobStates[rand.Intn(len(jobStates))])
			taskCountCol.Append(uint32(1 + rand.Intn(20)))
			cpuRequestCol.Append(0.5 + rand.Float64()*4.0)
			memoryRequestCol.Append(1.0 + rand.Float64()*8.0)
			createdAtCol.Append(now.Add(-time.Duration(rand.Intn(30*24)) * time.Hour))
			binaryCol.Append(binary)
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	jobCol.FinalizeColumn()
	cellCol.FinalizeColumn()
	clusterCol.FinalizeColumn()
	zoneCol.FinalizeColumn()
	ownerCol.FinalizeColumn()
	priorityCol.FinalizeColumn()
	stateCol.FinalizeColumn()
	taskCountCol.FinalizeColumn()
	cpuRequestCol.FinalizeColumn()
	memoryRequestCol.FinalizeColumn()
	createdAtCol.FinalizeColumn()
	binaryCol.FinalizeColumn()

	return t
}

// machineInfo for task assignment
type machineInfo struct {
	machine string
	rack    string
	cluster string
}

func getMachineList() []machineInfo {
	var result []machineInfo
	for _, r := range getRackList() {
		machinesInRack := 5 // Use fixed count for consistency
		for i := 0; i < machinesInRack; i++ {
			machine := fmt.Sprintf("%s-m%02d", r.rack, i)
			result = append(result, machineInfo{machine, r.rack, r.cluster})
		}
	}
	return result
}

// CreateGoogleTasksTable creates the tasks table
func CreateGoogleTasksTable(jobsTable *tables.DataTable) *tables.DataTable {
	t := tables.NewDataTable()

	taskCol := columns.NewStringColumn(columns.NewColumnDef("task", "Task", "google.task"))
	jobCol := columns.NewStringColumn(columns.NewColumnDef("job", "Job", "google.job"))
	cellCol := columns.NewStringColumn(columns.NewColumnDef("cell", "Cell", "google.cell"))
	machineCol := columns.NewStringColumn(columns.NewColumnDef("machine", "Machine", "google.machine"))
	rackCol := columns.NewStringColumn(columns.NewColumnDef("rack", "Rack", "google.rack"))
	clusterCol := columns.NewStringColumn(columns.NewColumnDef("cluster", "Cluster", "google.cluster"))
	stateCol := columns.NewStringColumn(columns.NewColumnDef("state", "State", ""))
	cpuUsageCol := columns.NewFloat64Column(columns.NewColumnDef("cpu_usage", "CPU Usage", ""))
	memoryUsageCol := columns.NewFloat64Column(columns.NewColumnDef("memory_usage_gb", "Memory Usage (GB)", ""))
	startTimeCol := columns.NewDatetimeColumn(columns.NewColumnDef("start_time", "Start Time", ""))
	restartsCol := columns.NewUint32Column(columns.NewColumnDef("restarts", "Restarts", ""))
	exitCodeCol := columns.NewUint32Column(columns.NewColumnDef("exit_code", "Exit Code", ""))

	t.AddColumn(taskCol)
	t.AddColumn(jobCol)
	t.AddColumn(cellCol)
	t.AddColumn(machineCol)
	t.AddColumn(rackCol)
	t.AddColumn(clusterCol)
	t.AddColumn(stateCol)
	t.AddColumn(cpuUsageCol)
	t.AddColumn(memoryUsageCol)
	t.AddColumn(startTimeCol)
	t.AddColumn(restartsCol)
	t.AddColumn(exitCodeCol)

	taskStates := []string{"running", "running", "running", "running", "starting", "dead", "preempted"}

	// Get machine list for assignment
	machineList := getMachineList()

	// Build cluster -> machines map for fast lookup
	clusterMachines := make(map[string][]machineInfo)
	for _, m := range machineList {
		clusterMachines[m.cluster] = append(clusterMachines[m.cluster], m)
	}

	now := time.Now()

	// Get jobs from the jobs table
	jobColSrc := jobsTable.GetColumn("job")
	cellColSrc := jobsTable.GetColumn("cell")
	clusterColSrc := jobsTable.GetColumn("cluster")
	taskCountColSrc := jobsTable.GetColumn("task_count")

	for i := 0; i < jobColSrc.Length(); i++ {
		jobName, _ := jobColSrc.GetString(uint32(i))
		cellName, _ := cellColSrc.GetString(uint32(i))
		clusterName, _ := clusterColSrc.GetString(uint32(i))
		taskCountStr, _ := taskCountColSrc.GetString(uint32(i))
		var taskCount int
		fmt.Sscanf(taskCountStr, "%d", &taskCount)

		// Get machines in this cluster
		machines := clusterMachines[clusterName]
		if len(machines) == 0 {
			continue
		}

		for taskIdx := 0; taskIdx < taskCount; taskIdx++ {
			// Pick a random machine from the cluster
			picked := machines[rand.Intn(len(machines))]

			state := taskStates[rand.Intn(len(taskStates))]

			taskCol.Append(fmt.Sprintf("%s/%d", jobName, taskIdx))
			jobCol.Append(jobName)
			cellCol.Append(cellName)
			machineCol.Append(picked.machine)
			rackCol.Append(picked.rack)
			clusterCol.Append(clusterName)
			stateCol.Append(state)
			cpuUsageCol.Append(rand.Float64() * 4.0)
			memoryUsageCol.Append(rand.Float64() * 8.0)
			startTimeCol.Append(now.Add(-time.Duration(rand.Intn(7*24)) * time.Hour))
			restartsCol.Append(uint32(rand.Intn(5)))
			if state == "dead" {
				exitCodeCol.Append(uint32(1 + rand.Intn(127)))
			} else {
				exitCodeCol.Append(uint32(0))
			}
		}
	}

	// Finalize columns to detect uniqueness and enable joins
	taskCol.FinalizeColumn()
	jobCol.FinalizeColumn()
	cellCol.FinalizeColumn()
	machineCol.FinalizeColumn()
	rackCol.FinalizeColumn()
	clusterCol.FinalizeColumn()
	stateCol.FinalizeColumn()
	cpuUsageCol.FinalizeColumn()
	memoryUsageCol.FinalizeColumn()
	startTimeCol.FinalizeColumn()
	restartsCol.FinalizeColumn()
	exitCodeCol.FinalizeColumn()

	return t
}

// CreateGoogleAllocsTable creates the allocs table
func CreateGoogleAllocsTable(tasksTable *tables.DataTable) *tables.DataTable {
	t := tables.NewDataTable()

	allocCol := columns.NewStringColumn(columns.NewColumnDef("alloc", "Alloc", "google.alloc"))
	taskCol := columns.NewStringColumn(columns.NewColumnDef("task", "Task", "google.task"))
	jobCol := columns.NewStringColumn(columns.NewColumnDef("job", "Job", "google.job"))
	machineCol := columns.NewStringColumn(columns.NewColumnDef("machine", "Machine", "google.machine"))
	resourceTypeCol := columns.NewStringColumn(columns.NewColumnDef("resource_type", "Resource Type", ""))
	requestedCol := columns.NewFloat64Column(columns.NewColumnDef("requested", "Requested", ""))
	limitCol := columns.NewFloat64Column(columns.NewColumnDef("limit", "Limit", ""))
	usedCol := columns.NewFloat64Column(columns.NewColumnDef("used", "Used", ""))
	unitCol := columns.NewStringColumn(columns.NewColumnDef("unit", "Unit", ""))

	t.AddColumn(allocCol)
	t.AddColumn(taskCol)
	t.AddColumn(jobCol)
	t.AddColumn(machineCol)
	t.AddColumn(resourceTypeCol)
	t.AddColumn(requestedCol)
	t.AddColumn(limitCol)
	t.AddColumn(usedCol)
	t.AddColumn(unitCol)

	taskColSrc := tasksTable.GetColumn("task")
	jobColSrc := tasksTable.GetColumn("job")
	machineColSrc := tasksTable.GetColumn("machine")

	allocIdx := 0
	for i := 0; i < taskColSrc.Length(); i++ {
		taskName, _ := taskColSrc.GetString(uint32(i))
		jobName, _ := jobColSrc.GetString(uint32(i))
		machineName, _ := machineColSrc.GetString(uint32(i))

		// CPU alloc
		allocCol.Append(fmt.Sprintf("alloc-%d", allocIdx))
		taskCol.Append(taskName)
		jobCol.Append(jobName)
		machineCol.Append(machineName)
		resourceTypeCol.Append("cpu")
		req := 0.5 + rand.Float64()*3.5
		requestedCol.Append(req)
		limitCol.Append(req * 1.5)
		usedCol.Append(req * rand.Float64())
		unitCol.Append("cores")
		allocIdx++

		// Memory alloc
		allocCol.Append(fmt.Sprintf("alloc-%d", allocIdx))
		taskCol.Append(taskName)
		jobCol.Append(jobName)
		machineCol.Append(machineName)
		resourceTypeCol.Append("memory")
		memReq := 1.0 + rand.Float64()*7.0
		requestedCol.Append(memReq)
		limitCol.Append(memReq * 1.5)
		usedCol.Append(memReq * rand.Float64())
		unitCol.Append("gb")
		allocIdx++

		// Disk alloc
		allocCol.Append(fmt.Sprintf("alloc-%d", allocIdx))
		taskCol.Append(taskName)
		jobCol.Append(jobName)
		machineCol.Append(machineName)
		resourceTypeCol.Append("disk")
		diskReq := 10.0 + rand.Float64()*90.0
		requestedCol.Append(diskReq)
		limitCol.Append(diskReq)
		usedCol.Append(diskReq * rand.Float64())
		unitCol.Append("gb")
		allocIdx++
	}

	// Finalize columns to detect uniqueness and enable joins
	allocCol.FinalizeColumn()
	taskCol.FinalizeColumn()
	jobCol.FinalizeColumn()
	machineCol.FinalizeColumn()
	resourceTypeCol.FinalizeColumn()
	requestedCol.FinalizeColumn()
	limitCol.FinalizeColumn()
	usedCol.FinalizeColumn()
	unitCol.FinalizeColumn()

	return t
}
