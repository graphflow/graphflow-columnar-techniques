# Contributing to Graphflow

## Reporting bugs

Report any bugs or suggestions by opening a [Github issue][github-issue].

## Contributing code

When you contribute code, you affirm that the contribution is your original 
work and that you license the work to the project under the project's open source 
license at the time of the submission. Whether or not you state this explicitly,
by submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license at 
the time of the submission and warrant that you have the legal authority to do so.

Send us your contributions by opening a [Github pull request][github-pull-request].

All contributions should conform to the following style guidelines.

### Java Style Guide

Our style guide rules are based on the [Google's Java Style Guide][java-style-guide].
However, any rule mentioned below overrides the one in the Google style guide.

#### Wrapping
* All code should be wrapped at **100 characters**.  
* Code goes to the end of the line before wrapping to the next line.  
* However, if it improves readability, code can be wrapped early, for instance, 
in complex *if* expressions and method calls.  
* When wrapping method calls, the *dot* operator, or the opening bracket should
 be placed in the preceding line.  

#### Blocks

* All blocks should be enclosed within curly braces, including single line *if* or 
loop statements.
* Curly braces for empty blocks should be placed in the same line as the declaration:
`class Graphflow {}`.

#### Optimizing Imports

* Before committing edits in git, for each edited file remove all unused imports. In IntelliJ this 
can be done by right clicking on the file and clicking "Optimize Imports". Do this by
right clicking on a specific file and not folders. Otherwise IntelliJ might optimize
all imports and you might end up editing files that should not be part of your commit.

### Git Style Guide

#### Commit Messages

* Use the present tense ("Add feature" not "Added feature").
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
* Limit the first line to 72 characters or less.

A good mnemonic for writing the first line of the commit message is to complete the
sentence: `This commit will ______`.

#### Development

* Always create a new branch on top of master corresponding to a new feature or change.  
`git checkout master`  
`git checkout -b new-feature`
* If you are working on a forked repository, you can use the master branch to add commits
and send pull requests. However, it is recommended that you use feature branches, and use
the master branch to keep in sync with new commits upstream.
* Always keep the feature branch rebased on top of master.    
`git rebase master new-feature`
* Feature branches are always merged to master using a new merge commit, as opposed
to using fast forward merges. 

Practices derived from [Keep a readable Git history][readable-git-history].

[java-style-guide]:https://google.github.io/styleguide/javaguide.html
[readable-git-history]:https://fangpenlin.com/posts/2013/09/30/keep-a-readable-git-history/
[github-issue]:https://github.com/graphflow/graphflow/issues
[github-pull-request]:https://github.com/graphflow/graphflow/pulls
