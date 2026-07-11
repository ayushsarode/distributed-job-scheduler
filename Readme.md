# Distributed Job Scheduler(in-progress)

![Architecture Design](design-docs/distributed-job-scheduler-design.png)

This project is a horizontally scalable distributed job scheduler built with Go and PostgreSQL. It is designed to reliably manage background jobs, handling everything from queuing and priority assignment to execution across multiple worker nodes.

The system features built-in worker lifecycle management. By using a heartbeat mechanism, it continuously tracks node health and automatically requeues any abandoned or stale jobs if a worker goes offline, ensuring high fault tolerance and consistent job processing.

<p align="center">
  <img src="https://komarev.com/ghpvc/?username=ayushsarode-distributed-job-scheduler&label=Views&color=0e75b6&style=flat" alt="Views" />
</p>
