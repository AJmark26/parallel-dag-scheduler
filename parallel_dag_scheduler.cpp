/**
 * Professional Parallel DAG Scheduler
 * Features:
 * 1. Thread Pool Execution
 * 2. Dependency Resolution (DAG)
 * 3. Fault Recovery (Retries)
 * 4. Optimization: Cascading Cancellation (skips tasks if parent fails)
 * 5. Safety: Cycle Detection
 */

#include <iostream>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <future>

// Enum for robust state tracking
enum class TaskStatus { PENDING, RUNNING, SUCCESS, FAILED, CANCELLED };

struct Task {
    int id;
    std::string name;
    std::function<void()> work;
    int maxRetries;
    std::atomic<int> attempts{0};
    std::atomic<TaskStatus> status{TaskStatus::PENDING};

    Task(int id, std::string name, std::function<void()> work, int maxRetries)
        : id(id), name(std::move(name)), work(std::move(work)), maxRetries(maxRetries) {}
};

class Scheduler {
private:
    std::unordered_map<int, std::shared_ptr<Task>> tasks;
    std::unordered_map<int, std::vector<int>> adj;     // Adjacency List
    std::unordered_map<int, int> inDegree;             // Dependency Counters

    // Synchronization
    std::mutex mtx;
    std::condition_variable cv;
    std::queue<int> readyQueue;
    std::vector<std::thread> workers;
    
    // Lifecycle management
    std::atomic<bool> running{false};
    std::atomic<int> activeTaskCount{0}; // Tracks pending + running tasks
    std::mutex completionMtx;
    std::condition_variable completionCv;

public:
    Scheduler() = default;

    // Destructor ensures threads are joined
    ~Scheduler() {
        if (running) {
            running = false;
            cv.notify_all();
            for (auto &t : workers) {
                if (t.joinable()) t.join();
            }
        }
    }

    void addTask(int id, std::string name, std::function<void()> work, int maxRetries = 0) {
        std::lock_guard<std::mutex> lock(mtx);
        tasks[id] = std::make_shared<Task>(id, std::move(name), std::move(work), maxRetries);
        // Ensure map entries exist
        if (inDegree.find(id) == inDegree.end()) inDegree[id] = 0;
        if (adj.find(id) == adj.end()) adj[id] = {};
    }

    void addDependency(int parent, int child) {
        std::lock_guard<std::mutex> lock(mtx);
        adj[parent].push_back(child);
        inDegree[child]++;
        // Ensure map entries exist
        if (inDegree.find(parent) == inDegree.end()) inDegree[parent] = 0;
        if (adj.find(child) == adj.end()) adj[child] = {};
    }

    bool start(int numThreads = std::thread::hardware_concurrency()) {
        std::unique_lock<std::mutex> lock(mtx);
        
        if (detectCycle()) {
            std::cerr << "[System] Cycle detected in DAG. Aborting.\n";
            return false;
        }

        activeTaskCount = tasks.size();
        
        // Populate initial Ready Queue (Nodes with In-Degree 0)
        for (auto const& [id, degree] : inDegree) {
            if (degree == 0 && tasks.count(id)) {
                readyQueue.push(id);
            }
        }

        running = true;
        for (int i = 0; i < numThreads; ++i) {
            workers.emplace_back(&Scheduler::workerLoop, this);
        }
        
        std::cout << "[System] Scheduler started with " << numThreads << " threads.\n";
        return true;
    }

    void waitForCompletion() {
        std::unique_lock<std::mutex> lock(completionMtx);
        completionCv.wait(lock, [this]{ return activeTaskCount == 0; });
        
        // Shutdown workers cleanly
        {
            std::lock_guard<std::mutex> lg(mtx);
            running = false;
        }
        cv.notify_all();
        for (auto &t : workers) {
            if (t.joinable()) t.join();
        }
        workers.clear();
    }

    void printReport() {
        std::lock_guard<std::mutex> lock(mtx);
        std::cout << "\n=== Execution Report ===\n";
        for (const auto& [id, task] : tasks) {
            std::string statusStr;
            switch (task->status) {
                case TaskStatus::SUCCESS: statusStr = "SUCCESS"; break;
                case TaskStatus::FAILED: statusStr = "FAILED"; break;
                case TaskStatus::CANCELLED: statusStr = "CANCELLED"; break;
                default: statusStr = "UNKNOWN"; break;
            }
            std::cout << "Task " << id << " (" << task->name << "): " << statusStr 
                      << " (Attempts: " << task->attempts << ")\n";
        }
        std::cout << "========================\n";
    }

private:
    void workerLoop() {
        while (true) {
            int taskId;
            std::shared_ptr<Task> task;

            {
                std::unique_lock<std::mutex> lock(mtx);
                cv.wait(lock, [this]{ return !running || !readyQueue.empty(); });
                
                if (!running && readyQueue.empty()) return; // Exit signal

                taskId = readyQueue.front();
                readyQueue.pop();
                task = tasks[taskId];
            }

            // Check if task was cancelled while waiting in queue
            if (task->status == TaskStatus::CANCELLED) {
                finalizeTask(taskId);
                continue;
            }

            task->status = TaskStatus::RUNNING;
            executeTask(task);
        }
    }

    void executeTask(std::shared_ptr<Task> task) {
        bool success = false;
        try {
            task->attempts++;
            task->work();
            success = true;
        } catch (const std::exception& e) {
            std::cerr << "[Error] Task " << task->id << " failed: " << e.what() << "\n";
        } catch (...) {
            std::cerr << "[Error] Task " << task->id << " unknown failure.\n";
        }

        if (success) {
            task->status = TaskStatus::SUCCESS;
            processTaskCompletion(task->id);
        } else {
            if (task->attempts <= task->maxRetries) {
                // RETRY LOGIC: Re-add to queue
                std::cout << "[System] Retrying Task " << task->id << "...\n";
                task->status = TaskStatus::PENDING;
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    readyQueue.push(task->id);
                }
                cv.notify_one();
            } else {
                // FAILURE LOGIC: Trigger Cascade Cancellation
                task->status = TaskStatus::FAILED;
                std::cerr << "[System] Task " << task->id << " failed permanently. Cascading cancellation...\n";
                cascadeCancel(task->id);
                finalizeTask(task->id);
            }
        }
    }

    void processTaskCompletion(int parentId) {
        std::lock_guard<std::mutex> lock(mtx);
        if (adj.find(parentId) != adj.end()) {
            for (int childId : adj[parentId]) {
                inDegree[childId]--;
                if (inDegree[childId] == 0) {
                    readyQueue.push(childId);
                    cv.notify_one();
                }
            }
        }
        finalizeTaskInternal();
    }

    // Recursively cancel children of a failed node
    void cascadeCancel(int parentId) {
        std::lock_guard<std::mutex> lock(mtx);
        std::vector<int> q = {parentId};
        std::unordered_set<int> visited; // Prevent cycles causing infinite loops
        
        while(!q.empty()) {
            int u = q.back(); 
            q.pop_back();

            if (adj.find(u) == adj.end()) continue;

            for (int child : adj[u]) {
                if (visited.count(child)) continue;
                visited.insert(child);

                if (tasks[child]->status == TaskStatus::PENDING || tasks[child]->status == TaskStatus::RUNNING) {
                    tasks[child]->status = TaskStatus::CANCELLED;
                    // We treat cancelled tasks as "done" for the counter
                    finalizeTaskInternal(); 
                }
                q.push_back(child);
            }
        }
    }

    // Helper to decrement active count and notify main thread
    void finalizeTask(int taskId) {
        std::lock_guard<std::mutex> lock(mtx);
        finalizeTaskInternal();
    }

    void finalizeTaskInternal() {
        activeTaskCount--;
        if (activeTaskCount == 0) {
            completionCv.notify_all();
        }
    }

    // Kahn's Algorithm for Cycle Detection
    bool detectCycle() {
        // Run on a copy so we don't mess up actual state
        auto inDegreeCopy = inDegree;
        std::queue<int> q;
        int visited = 0;

        for (auto const& [id, degree] : inDegreeCopy) {
            if (degree == 0) q.push(id);
        }

        while (!q.empty()) {
            int u = q.front();
            q.pop();
            visited++;
            
            if (adj.find(u) != adj.end()) {
                for (int v : adj[u]) {
                    inDegreeCopy[v]--;
                    if (inDegreeCopy[v] == 0) q.push(v);
                }
            }
        }

        return visited != tasks.size();
    }
};

int main() {
    Scheduler scheduler;

    // Define Tasks
    scheduler.addTask(1, "Load Data", []{ 
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); 
    });
    
    scheduler.addTask(2, "Process Data", []{ 
        std::this_thread::sleep_for(std::chrono::milliseconds(300)); 
    });
    
    // This task is designed to FAIL to demonstrate Cascading Cancellation
    scheduler.addTask(3, "Critical Service", []{ 
        throw std::runtime_error("Connection Timeout"); 
    }, 1); // 1 Retry allowed

    scheduler.addTask(4, "Save Result", []{ 
        std::cout << "Saving...\n"; 
    });

    scheduler.addTask(5, "Generate Report", []{ 
        std::cout << "Reporting...\n"; 
    });

    // Define Dependencies
    // 1 -> 2 -> 4
    // 1 -> 3 -> 5 (3 fails, so 5 should be CANCELLED)
    scheduler.addDependency(1, 2);
    scheduler.addDependency(1, 3);
    scheduler.addDependency(2, 4);
    scheduler.addDependency(3, 5);

    // Run
    scheduler.start(3); // 3 Threads
    scheduler.waitForCompletion();
    scheduler.printReport();

    return 0;
}