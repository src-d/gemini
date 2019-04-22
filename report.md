# Gemini usage report

I created this document to highlight what I think should be addressed before turning Gemini into a product and before working on it to include big features. I approached this in two ways:
	1. as a user who wants to try Gemini (without looking at the code),
	2. as a developer/contributor who wants to know how it's done under the hood and that wany eventually contribute to the project.

I realized that some of the considerations outlined below already have a corresponding issue on Github.

## As a user

1. The first thing that I'd do is to add a `Quickstart` section that let the user try Gemini through Docker with a couple of commands with his/her own repositories. Currently, there's the volume in `/repositories` path, but maybe it would easier to use an approach similar to Engine where we pass the repository to bind-mount.
2. `docker-compose up` seems to download the whole internet when building the images. This could annoy a potential user that just wants to give a quick try to Gemini. I'm not very familiar with SBT, but maybe we could reduce the uber jar to only build the dependencies that are required to run at least locally, and mark the other dependencies as provided at runtime. For example `gcs` and `hadoopAws`.
3. I had to add a couple of entries to `.dockerignore`, but I guess that we can't avoid this unless we do extensive research on what can cause problems depending on the different setups (in my case the problem was that I'm using Scala with Ensime that requires a global sbt plugin).
4. From the current `README.md` is not clear what to expect from each command `hash`, `query` and `report`:
	- `hash`:
		- is this a required step or is this used only to speed up the following operations? Or does this create a sort of workspace?
		- does this produce something on the file-system or in the DB?
	- `query`:
		- with which failes is the provided one being compared to? Maybe those passed to the `hash` command?
5. When I tried the `hash` command produced some Python errors, but running `query` and `report` was ok, so maybe better silence non-fatal errors?

## As a developer

1. The instructions for setting up the local environment are not very clear. For example, I didn't understand the purpose of the `DEV` env var. I'd improve the `Development` section in the `README.md`.
2. It seems that we're using a non-official version of `sbt`. I actually didn't take a deep look into it, but if they differ in something we should note and maybe also write a rationale behind this decision or consider using the official one.
3. Running the tests inside the container as `docker-compose exec Gemini ./sbt test` doesn't succeed.
4. By reading the code you early notice that one of the dependency is `package tech.sourced.engine`, but Engine doesn't actually have any Scala code. In the `README.md` Engine is cited so I'd expect that package to be present in there, but it turns out that the corresponding repo is actually `jgit-spark-connector`.
5. `jgit-spark-connector` is deprecated, so we should migrate to `gitbase-spark-connector`.
6. Many log messages use the `WARN` level.
7. As a potential contributor, I'd like to have some explanation about the design of the project and the rationale behind it:
	- I'd like to have an explanation on how the Python features extractors are called. Moreover, porting them to Scala would make the code cleaner and easier to debug. The same applies to the `report` command that calls some Python scripts.
	- I'd like to know what's exactly is being stored inside the database.
	- I'd like to have some high-level explanation on how everything works from an algorithmic point-of-view maybe even in layman terms. For example, I won't give it for granted that everybody knows what Locality Sensitive Hashing is. Currently, there's only a link to Apollo that has a link to a paper and a summary of the steps for detecting the duplicates.
8. To have  a proper opinion about refactors and tests I'd need to get more familiar with the codebase.

**It would be great to have some issues marked as "good first issue" that we could work on in order to start getting more familiar with the source code.**
