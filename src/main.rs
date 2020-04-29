//extern crate abomonation;
extern crate differential_dataflow;
extern crate timely;

use std::io::{Read, Write};

use crate::differential_dataflow::input::Input;
use crate::differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use crate::differential_dataflow::operators::JoinCore;
use crate::timely::scheduling::Scheduler;
use abomonation::abomonated::Abomonated;
use abomonation::{decode, encode};
//use differential_dataflow::trace::implementations::ord::OrdValSpineAbom as DefaultValTrace;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::Inspect;

//
// Open questions
// 1. Why does the batch have an upper of []?
// 2. How to write down the batch in a sink in a format we can read back in?
// 3. How do we read the batch data back in
//  a. it seems like at least in this toy we could interleave a timely source with some control flow
//  but its a scattered thought

//use abomonation::{encode, decode};

fn main() {
    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {
        // define a new computation.
        let mut input = worker.dataflow(|scope| {
            // create a new collection from our input.
            // Specifically: we get back a handle to throw stuff in and
            // the actual data
            let (handle, manages) = scope.new_collection();
            manages.inspect(|x| println!("manages: {:?}", x));
            let reverse = manages.map(|(manager, employee)| (employee, manager));
            let manages = manages.arrange_by_key();
            let reverse = reverse.arrange_by_key();

            //let reverse = reverse.arrange_by_key();

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .join_core(&reverse, |m2, m1, p| Some((*m2, *m1, *p)))
                .inspect(|x| println!("{:?}", x));

            //lets just print the stream of batches to stdout
            manages.stream.sink(Pipeline, "BatchPrinter", |input| {
                while let Some((time, data)) = input.next() {
                    for d in data.iter() {
                        //let local = (**d).clone();
                        let mut bytes = Vec::new();
                        unsafe {
                            encode(&(**d), &mut bytes);
                        }
                        println!("{:?}", d);
                        println!("{:?}", bytes);

                        // TODO in a more realistic scenario we would give this file a new name every time
                        let mut file = std::fs::File::create("testing-2")
                            .expect("creating file to dump batches");
                        file.write(&bytes);
                        file.flush();
                    }
                }
            });
            // Let's make a source to see if we can't read in this file
            source(scope, "BatchScanner", |capability, info| {
                let activator = scope.activator_for(&info.address[..]);

                let mut cap = Some(capability);
                move |output| {
                    let mut done = false;
                    if let Some(cap) = cap.as_mut() {
                        // get some data and send it.
                        let file = std::fs::File::open("testing").expect("open stored batch file");
                        let mut buf: Vec<u8> = Vec::new();
                        file.read_to_end(&mut buf).expect("read failed");

                        let batch = if let Some((batch, remaining)) =
                            unsafe { decode::<OrdValBatch<_, _, _, _>>(&mut buf) }
                        {
                            assert!(remaining.len() == 0);
                            println!("decoded: {:?}", batch);
                            batch
                        } else {
                            panic!("unable to decode batch");
                        };

                        let time: i32 = cap.time().clone();
                        output.session(&cap).give(batch);

                        // downgrade capability.
                        cap.downgrade(&(time + 1));
                        done = time > 20;
                    }

                    if done {
                        cap = None;
                    } else {
                        activator.activate();
                    }
                }
            })
            .inspect(|x| println!("{:?}", x));

            // return the handle so other non-dataflow code can feed us data
            handle
        });

        // Read a size for our organization from the arguments.
        let size = std::env::args().nth(1).unwrap().parse().unwrap();

        // Load input (a binary tree).
        input.advance_to(0);
        for person in 0..size {
            input.insert((person / 2, person));
        }

        for person in 1..size {
            input.advance_to(person);
            input.remove((person / 2, person));
            input.insert((person / 3, person));
        }
    })
    .expect("Computation terminated abnormally");
}
