# Adaptive HTTP Load Balancing Using Multi-Armed Bandits

### Reinforcement Learning for Adaptive Request Routing and Load Balancing

A research-backed engineering project exploring how **reinforcement learning (RL)**, particularly **Multi-Armed Bandits (MABs)**, can be applied to adaptive HTTP request routing. The goal is to build a working load balancer and understand how learning-based routing compares with established load balancing algorithms when backend performance changes over time.

## Problem

Load balancing plays an important role in distributing incoming requests across backend servers, but deciding where to send each request becomes difficult when server conditions are constantly changing. Traditional algorithms such as Round Robin follow predefined rules, while more advanced strategies consider current load or recent response times. Even these approaches have limitations when servers experience sudden slowdowns, traffic spikes, or unpredictable processing delays. Learning-based routing introduces another possibility: using feedback from previous requests to improve future decisions. However, this comes with its own challenges. A reinforcement learning algorithm may rely on outdated observations, receive feedback only after requests finish, or repeatedly choose a previously fast server until it becomes overloaded. The problem we want to investigate is whether lightweight RL algorithms can adapt effectively to these conditions and how their performance compares with existing load balancing techniques.

## Research That Shaped the Project

The project is grounded in existing research on adaptive routing, reinforcement learning, non-stationary multi-armed bandits, congestion, and delayed feedback. The following papers were particularly useful in understanding the problem and deciding what to build.

| Research Paper | Link |
|---|---|
| On Upper-Confidence Bound Policies for Non-Stationary Bandit Problems (2008) | [arXiv](https://arxiv.org/abs/0805.3415) |
| C3: Cutting Tail Latency in Cloud Data Stores via Adaptive Replica Selection (2015) | [USENIX NSDI](https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/suresh) |
| Congested Bandits: Optimal Routing via Short-term Resets (2022) | [ICML / PMLR](https://proceedings.mlr.press/v162/awasthi22a.html) |
| Backlogged Bandits: Cost-Effective Learning for Utility Maximization in Queueing Networks (2024) | [IEEE INFOCOM](https://ieeexplore.ieee.org/document/10621295/) |
| QoS-Aware Load Balancing in the Computing Continuum via Multi-Player Bandits (2026) | [arXiv](https://arxiv.org/abs/2512.18915) |
| Routing under Machine Breakdowns: A Benchmark of Dispatching Rules, Bandits, and Reinforcement Learning for Multi-Server Flow Shops (2026) | [Springer](https://doi.org/10.1007/s44444-026-00128-9) |

These studies helped establish the challenges we want to explore: adapting to non-stationary environments, handling congestion and delayed feedback, and evaluating learning-based routing against strong conventional alternatives.

## Objective

The objective is to **build and evaluate an adaptive HTTP load balancer using reinforcement learning techniques based on multi-armed bandits**. We want to understand how different routing algorithms behave when backend servers have unequal capacities, their performance changes, or incoming traffic creates congestion. By comparing traditional load balancing strategies with online learning algorithms under controlled workloads, we aim to identify where learning-based request routing performs well, where it struggles, and whether its additional complexity is justified.

## Proposed Solution

We will develop an HTTP reverse proxy in **Go**, with a modular routing engine that supports both traditional load balancing algorithms and multi-armed bandit policies. Each incoming request will be assigned to a backend according to the active routing algorithm. The proxy will track request outcomes, response latency, failures, and in-flight requests to provide the observations required by different policies.

For the reinforcement learning component, we will use **Multi-Armed Bandits**, a lightweight form of RL built around the exploration-exploitation trade-off. Each backend acts as an arm, and the algorithm learns from the performance of completed requests. Unlike deep reinforcement learning, these methods do not require neural networks or a separate training pipeline. They update their estimates online as requests are processed.

The system will run multiple configurable backend servers in Docker containers, allowing us to reproduce conditions such as performance degradation, recovery, heterogeneous processing speeds, and traffic bursts. We will generate HTTP workloads using k6 and analyze the results with Python, focusing on response latency, throughput, error rates, request distribution, and adaptation time.

The intention is to evaluate these approaches fairly, without assuming that reinforcement learning will always outperform conventional load balancing.

## Routing Algorithms

The project will implement and compare two categories of routing algorithms.

### Traditional and Adaptive Load Balancing

- **Round Robin:** Distributes requests sequentially across available backends.
- **Least Requests:** Selects the backend with the fewest in-flight requests.
- **Power of Two Choices (P2C):** Randomly samples two backends and selects the one with fewer active requests.
- **P2C + Peak EWMA:** Combines recent latency estimates with current request load to make performance-aware routing decisions.

### Reinforcement Learning: Multi-Armed Bandits

- **UCB1 (Upper Confidence Bound):** Balances exploration and exploitation using estimated rewards and uncertainty.
- **Sliding-Window UCB (SW-UCB):** Uses recent observations to adapt when backend performance changes.
- **Discounted UCB (D-UCB):** Gradually reduces the influence of older observations to respond to changing conditions.
- **Thompson Sampling (Optional):** Uses probabilistic sampling to balance exploration and exploitation.

The non-stationary bandit algorithms, particularly SW-UCB and D-UCB, are central to investigating how reinforcement learning handles changing backend conditions.

## Tech Stack

| Technology | Purpose |
|---|---|
| **Go (Golang)** | HTTP reverse proxy, routing engine, backend servers, and load balancing algorithms |
| **Go Standard Library** | HTTP networking, concurrency management, testing, and profiling |
| **Docker & Docker Compose** | Running and configuring multiple backend server containers |
| **Grafana k6** | HTTP load testing, traffic generation, and workload simulation |
| **Python** | Experimental analysis and evaluation of routing algorithms |
| **NumPy, pandas & SciPy** | Processing results, computing performance metrics, and statistical analysis |
| **Matplotlib** | Visualizing latency, throughput, adaptation, and algorithm comparisons |

The reinforcement learning algorithms will run directly inside the Go proxy to avoid additional network overhead during routing. Python will be used for offline analysis and experimentation.
