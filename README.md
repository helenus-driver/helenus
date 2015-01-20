# helenus
The Helenus Driver Project enables annotation of POJO classes and a driver layer to create statements (e.g. creating, updating, selecting from Cassandra tables, ...) by converting annotated POJO objects into actual CQL statements. It uses a JPA-like syntax for annotating POJO classes.

It defines a driver layer with a similar syntax to Cassandra's own Java driver.

It's current implementation actually sits on top of Cassandra Java layer. 
