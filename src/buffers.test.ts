import { describe, expect, it } from "vitest";
import { SampleRingBuffer, enforceSampleStoreLimit } from "./buffers";

describe("SampleRingBuffer", () => {
  it("keeps the newest samples at the per-channel limit", () => {
    const ring = new SampleRingBuffer(4);
    for (let index = 0; index < 7; index += 1) ring.push({ timeSeconds: index, value: index * 10, sequence: index });
    expect(ring.length).toBe(4);
    expect(ring.last(10).map((point) => point.value)).toEqual([30, 40, 50, 60]);
  });

  it("returns no more than two min/max points per pixel bucket", () => {
    const ring = new SampleRingBuffer(100);
    for (let index = 0; index < 100; index += 1) ring.push({ timeSeconds: index, value: Math.sin(index), sequence: index });
    expect(ring.downsampleSince(0, 12).length).toBeLessThanOrEqual(24);
  });

  it("enforces the total sample count across channels", () => {
    const first = new SampleRingBuffer(20);
    const second = new SampleRingBuffer(20);
    for (let index = 0; index < 12; index += 1) {
      first.push({ timeSeconds: index, value: index, sequence: index });
      second.push({ timeSeconds: index + 0.5, value: index, sequence: index });
    }
    const store = { first, second };
    enforceSampleStoreLimit(store, 16);
    expect(first.length + second.length).toBeLessThanOrEqual(16);
  });
});

