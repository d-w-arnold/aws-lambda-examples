# aws-lambda-examples

What is [AWS Lambda](https://aws.amazon.com/lambda/)?

This repo is a submodule of: [aws-cdk-examples](https://github.com/d-w-arnold/aws-cdk-examples)

### Add Git Hooks

See `pre-push` shell script in `hooks/`.

When pushing to the `main` branch, a push is successful unless Black formatter returns a non-zero exit code, in which it
will show the diff regarding what Black would change.

To utilise this pre-push git hook, run the following commands in the project root directory:

(Submodule repo)

```bash
# Copy all repo git hooks.
cp -av hooks/* ../.git/modules/aws-lambda/hooks

# Set all git hooks to executable, if not already set.
chmod +x ../.git/modules/aws-lambda/hooks/*
```

(Stand-alone repo)

```bash
# Copy all repo git hooks.
cp -av hooks/* .git/hooks

# Set all git hooks to executable, if not already set.
chmod +x .git/hooks/*
```
