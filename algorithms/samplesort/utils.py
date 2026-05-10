def split_data(arr, parts):
    chunk_size = len(arr) // parts
    chunks = []

    for i in range(parts):
        start = i * chunk_size

        if i == parts - 1:
            end = len(arr)
        else:
            end = (i + 1) * chunk_size

        chunks.append(arr[start:end])

    return chunks


def select_samples(sorted_chunk, sample_count):
    if len(sorted_chunk) == 0:
        return []

    step = max(1, len(sorted_chunk) // sample_count)

    return sorted_chunk[::step][:sample_count]


def choose_pivots(samples, parts):
    samples.sort()
    pivots = []

    step = len(samples) // parts

    for i in range(1, parts):
        pivots.append(samples[i * step])

    return pivots


def distribute_to_buckets(data, pivots):
    buckets = [[] for _ in range(len(pivots) + 1)]

    for value in data:
        inserted = False

        for i, pivot in enumerate(pivots):
            if value <= pivot:
                buckets[i].append(value)
                inserted = True
                break

        if not inserted:
            buckets[-1].append(value)

    return buckets