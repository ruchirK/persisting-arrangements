use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use abomonation::{decode, encode};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::{Count, JoinCore};
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::{BatchReader, Cursor};
use differential_dataflow::AsCollection;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::probe::{Handle, Probe};
use timely::dataflow::operators::{Capability, Inspect};
use timely::order::PartialOrder;
use timely::scheduling::Scheduler;

//
// Open questions
// 1. Why does the final batch have an upper of []?

fn main() {
    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        let file_prefix = "testing";
        let paths = std::fs::read_dir("./").unwrap();
        let mut recovery_ts: u64 = 0;
        let mut recovery_offset = 0;
        let mut delta = 10000;
        let mut files_to_read = BTreeSet::new();

        // We'll use normal dataflow to figure out which files we can recover from and
        // which offset to start reading from
        for path in paths {
            let filename = path
                .unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();

            if filename.starts_with(file_prefix) {
                let parts: Vec<_> = filename.split('-').collect();

                if parts.len() == 3 {
                    let ts = parts[1].parse::<u64>().unwrap();
                    let offset = parts[2].parse::<u64>().unwrap();

                    // We found a more recent recovery file, lets use it instead
                    if ts > recovery_ts {
                        recovery_ts = ts;
                        recovery_offset = 0;
                        files_to_read.clear();
                    }

                    if ts == recovery_ts {
                        files_to_read.insert(filename);
                        if offset > recovery_offset {
                            recovery_offset = offset;
                        }
                    }
                }
            }
        }

        println!(
            "reading from offset: {}, will read to: {}",
            recovery_offset,
            recovery_offset + delta
        );
        // In this dataflow we are reading a previously saved set of batches from
        // disk, rearranging it and setting everything to start before any updates we will receive
        // and sending it to be shared to other dataflow computations
        // Input: Vec<Path> or something like that to know which files to read
        // Output: An arrangement containing all of the data in those stored batches, consolidated to
        // an artificially early timestamp
        let mut old_batch = worker.dataflow::<u32, _, _>(|scope| {
            source::<_, _, _, _>(scope, "BatchReader", |mut capability, info| {
                let activator = scope.activator_for(&info.address[..]);

                let mut cap = Some(capability);
                move |output| {
                    if let Some(cap) = cap.as_mut() {
                        let mut time = cap.time().clone();
                        for f in files_to_read.iter() {
                            // get some data and send it.
                            println!("opening {} to load up a stored batch", f);
                            let mut file = std::fs::File::open(f).expect("open stored batch file");
                            let mut buf: Vec<u8> = Vec::new();
                            file.read_to_end(&mut buf).expect("read failed");

                            // TODO unclear how completely we need to specify types
                            // for OrdValBatch here
                            let batch = if let Some((batch, remaining)) =
                                unsafe { decode::<OrdValBatch<i32, i32, i32, isize>>(&mut buf) }
                            {
                                assert!(remaining.len() == 0);
                                //println!("decoded: {:?}", batch);
                                batch
                            } else {
                                // TODO In the future we'll need to be able to handle this
                                // more gracefully
                                panic!("unable to decode batch");
                            };

                            let mut session = output.session(&cap);
                            // Step through all the (key, val, time, diff) tuples in this batch
                            // and send them to our computation
                            let mut cursor = batch.cursor();
                            cursor.rewind_keys(&batch);
                            cursor.rewind_vals(&batch);

                            while cursor.key_valid(&batch) {
                                let key = cursor.key(&batch);
                                while cursor.val_valid(&batch) {
                                    let val = cursor.val(&batch);
                                    cursor.map_times(&batch, |ts, r| {
                                        if time.less_than(&(*ts as u32)) {
                                            time = *ts as u32;
                                        }
                                        session.give((
                                            (key.clone(), val.clone()),
                                            // XXX this is a total hack
                                            // but the intuition is - if updates can be brought
                                            // forward to a time on the frontier
                                            // why can't they go backwards? (before any execution)
                                            // Make it so that all updates from saved state present as happening
                                            // at Timestamp::minimum() TODO actually use that API
                                            // The goal is to let us use a saved arrangement without having
                                            // to coordinate with other dataflows about which timestamp to go
                                            // forwards to
                                            // I think we may be able to achieve a similar transform via going forwards
                                            // in time with delay
                                            0,
                                            r.clone(),
                                        ));
                                    });
                                    cursor.step_val(&batch);
                                }
                                cursor.step_key(&batch);
                            }
                        }
                        cap.downgrade(&time);
                    }
                    // downgrade capability.
                    activator.activate();
                    cap = None;
                }
            })
            //.probe_with(&mut probe)
            .as_collection()
            //.consolidate()
            //.inspect(|x| println!("rereading {:?}", x))
            .arrange_by_key()
            .trace
        });

        // Let's use the saved state in a new computation and advance further
        // We will approximate "reading from kafka" via an infinite loop that
        // generates some inserts and deletes. Maybe initially to keep it simple
        // we just generate inserts?
        // Inputs:
        // - starting point: some i32 x to tell us where to begin
        // - old batch data
        // - a place to write batches down to
        worker.dataflow::<u32, _, _>(|scope| {
            let manages = source(scope, "DataInput", |mut capability, info| {
                let activator = scope.activator_for(&info.address[..]);
                let handle = probe.clone();

                let mut cap = Some(capability);
                let mut x: i32 = (recovery_offset + 1) as i32;
                move |output| {
                    let mut done = false;

                    if let Some(cap) = cap.as_mut() {
                        let mut time = cap.time().clone();
                        {
                            let mut session = output.session(&cap);
                            session.give(((x / 2, x), time, 1));
                            session.give(((x / 2, x), time, -1));
                            session.give(((x / 3, x), time, 1));
                        }

                        if x % 100 == 0 {
                            cap.downgrade(&(time + 1));
                        }
                        x += 1;

                        done = x > (recovery_offset + delta + 1) as i32;
                    }

                    if done {
                        cap = None;
                    } else {
                        activator.activate()
                    }
                }
            })
            //.probe_with(&mut input_probe)
            .as_collection();
            //.inspect(|x| println!("stream {:?}", x));

            // Converting my imported trace back to a collection because I want to
            // concat it back into the original, and I don't have a concat-like
            // operator over arrangements
            let old = old_batch.import(scope).as_collection(|k, v| (*k, *v));
            let manages = manages.concat(&old).consolidate();

            // We'll use this dataflow to check if we got a duplicate record
            manages
                .map(|(k, v)| ((k, v), ()))
                .count()
                .filter(|(_, count)| *count != 1)
                .inspect(|x| println!("error: got count that wasn't one: {:?}", x));
            //.probe_with(&mut probe)
            //.inspect(|x| println!("concat old + manages {:?}", x));
            let reverse = manages.map(|(manager, employee)| (employee, manager));

            // Let's store and bring back these collections because we have a good idea
            // how they evolve
            let manages = manages.arrange_by_key();
            let reverse = reverse.arrange_by_key();

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .join_core(&reverse, |m2, m1, p| Some((*m2, *m1, *p)))
                .probe_with(&mut probe);

            // We'll use a sink to write down the stream of batches we
            // get from the manages collection
            // Lets figure out a timestamp to identify these files with
            let startup_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("just tell me what time it is")
                .as_secs();
            manages.stream.sink(Pipeline, "BatchWriter", move |input| {
                while let Some((time, data)) = input.next() {
                    for d in data.iter() {
                        // Need to figure out the offset. We will cheat and do so by looking at all of the records in this batch
                        // to find the one with the largest v (given (k, v))
                        let mut offset = 0;

                        let mut cursor = d.cursor();
                        cursor.rewind_keys(&d);
                        cursor.rewind_vals(&d);

                        while cursor.key_valid(&d) {
                            while cursor.val_valid(&d) {
                                let val = cursor.val(d);
                                if *val > offset {
                                    offset = *val;
                                }
                                cursor.step_val(&d);
                            }
                            cursor.step_key(&d);
                        }

                        let mut bytes = Vec::new();
                        unsafe {
                            encode(&(**d), &mut bytes).expect("encoding batches failed");
                        }
                        println!("{:?} {:?}", d.description(), time);
                        //println!("{:?}", bytes);

                        let filename = format!("testing-{}-{}", startup_time, offset);
                        let mut file = std::fs::File::create(filename)
                            .expect("creating file to write batches");
                        file.write(&bytes).expect("writing batches should succeed");
                        file.flush().expect("flushing batches should succeed");
                    }
                }
            });
        });

        drop(old_batch);
    })
    .expect("Computation terminated abnormally");
}
