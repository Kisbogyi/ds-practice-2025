# Documentation

This folder should contain your documentation, explaining the structure and content of your project. It should also contain your diagrams, explaining the architecture. The recommended writing format is Markdown.

## Architecture diagrams
![Architecture Diagram](./Images/Architecture_diagram.svg)

The orangeish brown components represent components that are only connected to
the internal network, which means that they are only accessible from the internal
"docker" network. This is done because we want to minimize the possible attack
surface. 


## Sequence diagrams

### Ordering a book
First the webserver returns the website to the client

[![](./Images/get_website_sequence.png)](https://editor.plantuml.com/uml/SoWkIImgAStDuN8goYylJYrIqBLJS2lApoj9pKknKd1t3T7dWd4LT85oK6LM2aMf9QL5USKAvQb9nPabgLnS3a0b88C0)

Then the client sends the data to the api.
The orchestrator initiates the microservices then starts thoose that can be
started with the first vector clock broadcast.

[![](./Images/init_sequence.png)](https://editor.plantuml.com/uml/XP9DJiCm48NtFiMe2mIB27MB2AeWIiMgbH9bMmsEQQswTcHFG6B58JWX9s5iVYIN5BRlUs_-x4KWz3GU30mx36g-qLQJ84r6mJegC8VtrpUuPrcZqIz8sbauWqv9SXxkZ9DxkFKEUuc1X1Yud9PwG4jHVWxAPzhAovqAv97J6PZL7iSUAaLScNlC00FKLUvfFTg0YVC4NZzgYJ_cjilCpRZTSYUc8T9cAKHP7YzopWaqHfaBA4iXlYQ6uYOR5AvWlMfQk6H9xjt8UsQOxNN5fdIyZ5mNnIUIxZ0O5NjkdkJlUDkTZ2VqJxnPdemdT1JdEqo_Nph2v6eFtvPOO7BwFnrfYimX5ihs_F4-0000)

The Fraud Detection and Transaction verification does their work paralelly and 
then if they both say everything is O.k. Suggestions start to work.

[![](./Images/vc_sequence.png)](https://editor.plantuml.com/uml/VPEzJiCm58LtFyLH9nWoqB4Yg1BGZbIawbfT73UrABPbEuXkF0HFo9DmuxIXwT-ImUVO_iwvpZX4yh7-sC2jAQZXqYdbbs5hf8WwRXHcpJA6-Ft-mOmr5ApXPg5aj1wF6olVyVmbrsGgTIk4uzEqr8vCn8kNQnMY9tRsXC74KrtYIKK-HLip00LCdlgUkISJAEbOAAzNMb9ddYzwvgAkAhw9rT2enLI8FCUhhKs9knRTQ-jrt88gPM90an1vBaJbRUrm18IcXUGHO31a3tmZHdZpbaf98Iu_TyfzbZKmUrSnlUXYAFGgIQiiZk1LS8okZWqPdyYrA5FkEI-LqWDcrBJn58MLzjBETGvuvpeFd7gu1inj9E1k0EtREHnHmyc3MaLcTaU3-P_9uOx3U8ny8HuhNRpMTMvgErUQsacyrCUrUDnAYZVcB_ysVm00)

After all of the microservices returns an order is enqued and then the API return to
the user. The enqued order is processed in the background by Order Executor 
microservice.

[![](./Images/que_sequence.png)](https://editor.plantuml.com/uml/RP5DJiD038NtSmghYmeaM7K74Yf8aTY5iAvK3PFN9jBK4yOpWUuu12Va93W_e1QnjR_t_6ol9TcOyi5JrrZ1QlkQaM5IcppeQIZHYhu-FgaQwlHekGwx7Itf6T855XZJgK7hsivoeeSOtWJnh5P6rq1Ij2cSDnPbh14dvmLPeRBwhr-ymsNLJaXX38S42hssJgtfXkwz9xk7IkESJZRSs1xq0Z2vu3rSGctCcV3wJYUh73mkJs4O-mTHPjvDmH7OwkLI5hgmScHtUHfh9gycsnzTX4FRGtx3tMxcWLqC3Y8jxpKZfvQpKgDj4mE7BFweaQinQA6EDIRTaZ3UHf-m1jVwnMy0)


## Leader election
There are multiple leader election algorithms that we can use to synchronise
our order executors. These algorithms include Bullying, Ring election, Paxos or Raft.
These algorithms have their specific usecases where they are applicable. 

Bullying is one of the simplest leader election algorithm that relies on a hierarchy
like node ids or ip addresses. If the healthcheck fails then the nodes ask each 
node that has a higher status if they are available, and if no node responded 
than they become the leader. The disadvantage for this algorithm is that this 
scales up badly with the node counts, so it is not a good choice if the system have 
a lot of nodes or the nodes are failig frequently.

Ring algorithm uses a ring structure where each node stores its successor, like 
in a linked list. If a node failes the healthcheck then a node sends an election
message to the next node which forwards it if it also thinks that the leader is 
not available. This is continued until the highest priority node is reached and 
then it becomes the leader. This is a great algorithm for nodes that can be arranged
into a ring structure. It has low overhead, but the ring can easily be destrupted.

Paxos is not a leader election algorithm, but a consensus algorithm, where the 
nodes have to agree on a valid value. Here the nodes has specific roles that 
they have to fulfill. This algortihm is complex but it can scale better, and it
can work in split brain systems with multi-paxos.

Raft is a protocol for leader election and log replication. It has high faliure 
tolerance but it is signigicantly harder to implement than Bullying.

### Bullying
Our choice was bullying for the following reasons. The specifications say that 
the algorithm should currently only work with 2 nodes. Which is not a lot, these 
nodes are in one single system, so network failures are not common. Therefore 
we went with the KISS principle keep it simple stupid. This makes that our implementation
has fewer bugs and it's easier to understand/maintain. In the future if this algorithm 
is not enough we can still replace it with a different one easily.

![Healthcheck fails](./Images/bullying1.svg)
![Election sent from 3](./Images/bullying2.svg)
![Election sent from 2](./Images/bullying3.svg)
![Coordination sent](./Images/bullying4.svg)

