Material for the "An Introduction to Streaming (in Python)" talk.

To view the slides, open [this](https://ncreep.github.io/python-streaming-intro/presentation/presentation.html) link (click p to show the speaker's notes).

Original Scala/fs2 version can be found [here](https://github.com/ncreep/fs2-streaming-intro).

## Abstract

You may have heard about streaming, maybe someone told you to use it when your program was out of memory. But today we'll learn that there's much more to streaming than that.

Streaming is an immensely powerful technique that can help us structure complex flows into simple, testable, and modular components. By learning how to think in a streaming fashion we will manage to tame complex concurrency, scary state management, and intricate timing. All wrapped up behind an elegant and highly reusable interface.


## Setup and checks

The code examples are under `code/` From the repository root, run:

```bash
cd code
uv sync
```

## Examples

Run these commands from `code/`:

```bash
uv run python -m workshop_stream.temperature
uv run python -m workshop_stream.intro_examples
uv run python -m workshop_stream.in_memory
uv run python -m workshop_stream.better_api_calls_imperative
uv run python -m workshop_stream.from_console
uv run python -m workshop_stream.from_file
```

### Network example

Start the server from `code/` in one terminal:

```bash
uv run python -m workshop_stream.from_network
```

In another terminal, from the repository root, send newline-delimited CSV to
the server at `localhost:5555`:

```bash
printf 'Alice,2,false\nBob,3,true\n' | nc localhost 5555
