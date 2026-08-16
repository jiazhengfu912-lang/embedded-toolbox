import type { PlotPoint } from "./types";

const INITIAL_CAPACITY = 1024;

export class SampleRingBuffer {
  private times: Float64Array;
  private values: Float64Array;
  private sequences: Float64Array;
  private head = 0;
  private count = 0;
  readonly maxLength: number;

  constructor(maxLength = 200_000) {
    this.maxLength = Math.max(1, maxLength);
    const capacity = Math.min(INITIAL_CAPACITY, this.maxLength);
    this.times = new Float64Array(capacity);
    this.values = new Float64Array(capacity);
    this.sequences = new Float64Array(capacity);
  }

  get length(): number { return this.count; }
  get byteCapacity(): number { return this.times.byteLength + this.values.byteLength + this.sequences.byteLength; }
  get firstTime(): number { return this.count ? this.times[this.head] : Number.POSITIVE_INFINITY; }
  get lastTime(): number { return this.count ? this.times[this.index(this.count - 1)] : 0; }

  push(point: PlotPoint): void {
    if (this.count === this.times.length && this.times.length < this.maxLength) this.grow();
    if (this.count === this.maxLength) {
      this.write(this.head, point);
      this.head = (this.head + 1) % this.times.length;
      return;
    }
    this.write(this.index(this.count), point);
    this.count += 1;
  }

  dropOldest(amount: number): void {
    const removed = Math.min(Math.max(0, amount), this.count);
    this.head = (this.head + removed) % this.times.length;
    this.count -= removed;
  }

  compact(): void {
    let capacity = Math.min(INITIAL_CAPACITY, this.maxLength);
    while (capacity < this.count && capacity < this.maxLength) capacity *= 2;
    capacity = Math.min(capacity, this.maxLength);
    if (capacity >= this.times.length) return;
    const times = new Float64Array(capacity);
    const values = new Float64Array(capacity);
    const sequences = new Float64Array(capacity);
    for (let offset = 0; offset < this.count; offset += 1) {
      const source = this.index(offset);
      times[offset] = this.times[source];
      values[offset] = this.values[source];
      sequences[offset] = this.sequences[source];
    }
    this.times = times;
    this.values = values;
    this.sequences = sequences;
    this.head = 0;
  }

  last(limit: number): PlotPoint[] {
    const start = Math.max(0, this.count - limit);
    const points: PlotPoint[] = [];
    for (let offset = start; offset < this.count; offset += 1) points.push(this.read(offset));
    return points;
  }

  downsampleSince(since: number, width: number): PlotPoint[] {
    let start = this.count;
    while (start > 0 && this.times[this.index(start - 1)] >= since) start -= 1;
    const length = this.count - start;
    if (length <= 0) return [];
    const targetBuckets = Math.max(1, Math.floor(width));
    const bucketSize = Math.max(1, Math.ceil(length / targetBuckets));
    const output: PlotPoint[] = [];
    for (let offset = start; offset < this.count; offset += bucketSize) {
      const end = Math.min(this.count, offset + bucketSize);
      let min = this.read(offset);
      let max = min;
      for (let cursor = offset + 1; cursor < end; cursor += 1) {
        const point = this.read(cursor);
        if (point.value < min.value) min = point;
        if (point.value > max.value) max = point;
      }
      if (min.timeSeconds <= max.timeSeconds) output.push(min, max);
      else output.push(max, min);
    }
    return output;
  }

  private index(offset: number): number { return (this.head + offset) % this.times.length; }

  private read(offset: number): PlotPoint {
    const index = this.index(offset);
    return { timeSeconds: this.times[index], value: this.values[index], sequence: this.sequences[index] };
  }

  private write(index: number, point: PlotPoint): void {
    this.times[index] = point.timeSeconds;
    this.values[index] = point.value;
    this.sequences[index] = point.sequence;
  }

  private grow(): void {
    const capacity = Math.min(this.maxLength, this.times.length * 2);
    const times = new Float64Array(capacity);
    const values = new Float64Array(capacity);
    const sequences = new Float64Array(capacity);
    for (let offset = 0; offset < this.count; offset += 1) {
      const source = this.index(offset);
      times[offset] = this.times[source];
      values[offset] = this.values[source];
      sequences[offset] = this.sequences[source];
    }
    this.times = times;
    this.values = values;
    this.sequences = sequences;
    this.head = 0;
  }
}

export type SampleStore = Record<string, SampleRingBuffer>;

export function enforceSampleStoreLimit(store: SampleStore, maxSamples = 1_000_000, maxBytes = 64 * 1024 * 1024): void {
  let total = Object.values(store).reduce((sum, ring) => sum + ring.length, 0);
  let allocated = Object.values(store).reduce((sum, ring) => sum + ring.byteCapacity, 0);
  while (total > maxSamples || allocated > maxBytes) {
    const oldest = Object.values(store).filter((ring) => ring.length > 0).sort((a, b) => a.firstTime - b.firstTime)[0];
    if (!oldest) break;
    const remove = Math.min(oldest.length, Math.max(1, total - maxSamples, 1024));
    oldest.dropOldest(remove);
    total -= remove;
    for (const ring of Object.values(store)) ring.compact();
    allocated = Object.values(store).reduce((sum, ring) => sum + ring.byteCapacity, 0);
  }
}
