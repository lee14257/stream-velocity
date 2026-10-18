# A small, self-contained image that installs the library and runs its
# benchmarks. Built on Alpine Linux so the image itself stays lightweight.

FROM python:3.11-alpine

WORKDIR /app

# Copy the project files into the image.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY tests ./tests

# Install the library from source.
RUN pip install --no-cache-dir .

# When the container starts, run the speed benchmark to confirm it stays fast.
CMD ["python", "tests/benchmark_latency.py"]
