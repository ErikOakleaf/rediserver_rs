use criterion::{Criterion, criterion_group, criterion_main};
use redis::redis::zip_list::{ZipEntry, ZipList};
use std::hint::black_box;

fn bench_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("ziplist_push");

    let s6 = b"hello".to_vec().into_boxed_slice();
    let s14 = vec![b'a'; 100].into_boxed_slice();
    let s32 = vec![b'b'; 20_000].into_boxed_slice();

    group.bench_function("all_types", |b| {
        b.iter_batched(
            || ZipList::new(), // setup
            |mut zl| {
                // Bench
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100_000));
                zl.push(ZipEntry::Int32(2_147_483_647));
                zl.push(ZipEntry::Int64(5_000_000_000));

                // Clone because Box<[u8]> is owned.
                zl.push(ZipEntry::Str6BitsLength(s6.clone()));
                zl.push(ZipEntry::Str14BitsLength(s14.clone()));
                zl.push(ZipEntry::Str32BitsLength(s32.clone()));
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("ziplist_insert");

    let s6 = b"hello".to_vec().into_boxed_slice();
    let s14 = vec![b'a'; 100].into_boxed_slice();
    let s32 = vec![b'b'; 20_000].into_boxed_slice();

    group.bench_function("all_types_at_middle", |b| {
        b.iter_batched(
            || {
                // Setup
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int8(0));
                zl.push(ZipEntry::Int8(99));
                zl
            },
            |mut zl| {
                // Bench
                zl.insert(1, ZipEntry::Int4BitsImmediate(5));
                zl.insert(1, ZipEntry::Int8(100));
                zl.insert(1, ZipEntry::Int16(1000));
                zl.insert(1, ZipEntry::Int24(100000));
                zl.insert(1, ZipEntry::Int32(2147483647));
                zl.insert(1, ZipEntry::Int64(5000000000));

                zl.insert(1, ZipEntry::Str6BitsLength(s6.clone()));

                zl.insert(1, ZipEntry::Str14BitsLength(s14.clone()));

                zl.insert(1, ZipEntry::Str32BitsLength(s32.clone()));
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("ziplist_delete");

    group.bench_function("delete", |b| {
        b.iter_batched(
            || {
                // Setup
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100000));
                zl.push(ZipEntry::Int32(2147483647));
                zl.push(ZipEntry::Int64(5000000000));

                let s6 = b"hello".to_vec().into_boxed_slice();
                zl.push(ZipEntry::Str6BitsLength(s6));

                let s14 = vec![b'a'; 100].into_boxed_slice();
                zl.push(ZipEntry::Str14BitsLength(s14));

                let s32 = vec![b'b'; 20000].into_boxed_slice();
                zl.push(ZipEntry::Str32BitsLength(s32));

                zl
            },
            |mut zl| {
                // Bench
                for _ in 0..9 {
                    zl.remove(0);
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("delete_tail", |b| {
        b.iter_batched(
            || {
                // Setup
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100000));
                zl.push(ZipEntry::Int32(2147483647));
                zl.push(ZipEntry::Int64(5000000000));

                let s6 = b"hello".to_vec().into_boxed_slice();
                zl.push(ZipEntry::Str6BitsLength(s6));

                let s14 = vec![b'a'; 100].into_boxed_slice();
                zl.push(ZipEntry::Str14BitsLength(s14));

                let s32 = vec![b'b'; 20000].into_boxed_slice();
                zl.push(ZipEntry::Str32BitsLength(s32));

                zl
            },
            |mut zl| {
                // Bench
                for _ in 0..9 {
                    zl.remove_tail();
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("ziplist_get");

    group.bench_function("all_types", |b| {
        b.iter_batched(
            || {
                // Setup
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100000));
                zl.push(ZipEntry::Int32(2147483647));
                zl.push(ZipEntry::Int64(5000000000));

                let s6 = b"hello".to_vec().into_boxed_slice();
                zl.push(ZipEntry::Str6BitsLength(s6));

                let s14 = vec![b'a'; 100].into_boxed_slice();
                zl.push(ZipEntry::Str14BitsLength(s14));

                let s32 = vec![b'b'; 20000].into_boxed_slice();
                zl.push(ZipEntry::Str32BitsLength(s32));

                zl
            },
            |mut zl| {
                // Bench
                for i in 0..9 {
                    black_box(zl.get(i));
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

pub fn bench_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("ziplist_pop");

    group.bench_function("pop_head", |b| {
        b.iter_batched(
            || {
                // Setup: ZipList with one of each kind
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100000));
                zl.push(ZipEntry::Int32(2147483647));
                zl.push(ZipEntry::Int64(5000000000));

                let s6 = b"hello".to_vec().into_boxed_slice();
                zl.push(ZipEntry::Str6BitsLength(s6));

                let s14 = vec![b'a'; 100].into_boxed_slice();
                zl.push(ZipEntry::Str14BitsLength(s14));

                let s32 = vec![b'b'; 20000].into_boxed_slice();
                zl.push(ZipEntry::Str32BitsLength(s32));

                zl
            },
            |mut zl| {
                // Bench: pop all elements from head
                for _ in 0..9 {
                    let _ = zl.pop_head();
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("pop_tail", |b| {
        b.iter_batched(
            || {
                // Setup: same ZipList as above
                let mut zl = ZipList::new();
                zl.push(ZipEntry::Int4BitsImmediate(5));
                zl.push(ZipEntry::Int8(100));
                zl.push(ZipEntry::Int16(1000));
                zl.push(ZipEntry::Int24(100000));
                zl.push(ZipEntry::Int32(2147483647));
                zl.push(ZipEntry::Int64(5000000000));

                let s6 = b"hello".to_vec().into_boxed_slice();
                zl.push(ZipEntry::Str6BitsLength(s6));

                let s14 = vec![b'a'; 100].into_boxed_slice();
                zl.push(ZipEntry::Str14BitsLength(s14));

                let s32 = vec![b'b'; 20000].into_boxed_slice();
                zl.push(ZipEntry::Str32BitsLength(s32));

                zl
            },
            |mut zl| {
                // Bench: pop all elements from tail
                for _ in 0..9 {
                    let _ = zl.pop_tail();
                }
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_push, bench_insert, bench_delete, bench_get, bench_pop);
criterion_main!(benches);
