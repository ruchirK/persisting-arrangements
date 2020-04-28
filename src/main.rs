//extern crate abomonation;
extern crate differential_dataflow;
extern crate timely;

use crate::differential_dataflow::input::Input;
use crate::differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use crate::differential_dataflow::operators::JoinCore;
use abomonation::abomonated::Abomonated;
use abomonation::encode;
//use differential_dataflow::trace::implementations::ord::OrdValSpineAbom as DefaultValTrace;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;

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
            manages
                .stream
                .sink(Pipeline, "StdOutBatchPrinter", |input| {
                    while let Some((time, data)) = input.next() {
                        for d in data.iter() {
                            //let local = (**d).clone();
                            let mut bytes = Vec::new();
                            unsafe {
                                encode(&(**d), &mut bytes);
                            }
                            println!("{:?}", d);
                            println!("{:?}", bytes);
                        }
                    }
                });
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
        input.advance_to(1);
    })
    .expect("Computation terminated abnormally");
}
