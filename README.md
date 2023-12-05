# Introduction
This repository contains examples of using Polars and Deltalake directly against ADLSGen2 Storage. For maximum reproducibility they use the base interfaces of azure storage and minimal dependencies are added (no AzureML Data).

The repository uses poetry to manages dependencies instead of pip. Poetry offers a much richer configuration environment files via pyproject.tml and poetry.lock. Unlike pip, poetry.lock locks the dependencies at binary level and in a hasheable format, ensuring even better reproducibility. Besides it allow us to combine nicely with pre-commit hooks and linting tools like ruff or pylint.

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:

1.	Installation of poetry
2.	Configuring poetry
3.	Installation of dependencies

# Build and Test
TODO: Describe and show how to build your code and run the tests.

# Contribute
TODO: Explain how other users and developers can contribute to make your code better.

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)
