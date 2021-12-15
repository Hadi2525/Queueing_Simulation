# Queueing_Simulation
Discrete Event-based Simulation of Queueing Systems

------------------------------------------------------
This repository presents The basics of simulating various types of queueing systems. To understand the concept of queueing theory refer to [this link](https://en.wikipedia.org/wiki/Queueing_theory).

<details>
           <summary>Simulation of G/G/K/K (M/M/K/K or M/G/K/K) Queueing System FCFS </summary>
           <p>Suppose that there is one queueing system with limited number of servers (K) and any incoming flow beyond the capacity of the system (K) are rejected.</p>
</details>

<details>
           <summary>Simulation of G/G/1/PS (M/M/1/PS or M/G/1/PS) Queueing System</summary>
           <p>In this case, let's assume that we have one fast server and we have arrival users with general distribution and each user has a service time requirement following another general distribution. The servicing policy is that the processor of our main server is shared between customers in the system. Examples of this is when we have one fast cpu in a cloud computing server and the clock is shared between jobs arriving at this server. The higher the number of jobs present in the server, the slower it the processing gets and vice versa. </p>
</details>
