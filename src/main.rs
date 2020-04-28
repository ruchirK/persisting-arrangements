extern crate differential_dataflow;
extern crate timely;

use crate::differential_dataflow::input::Input;
use crate::differential_dataflow::operators::arrange::Arrange;
use crate::differential_dataflow::operators::JoinCore;
use differential_dataflow::trace::implementations::ord::OrdValSpineAbom as DefaultValTrace;
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
            let reverse = manages.map(|(manager, employee)| (employee, manager));
            let manages = manages.arrange::<DefaultValTrace<_, _, _, _, u32>>();
            let reverse = reverse.arrange::<DefaultValTrace<_, _, _, _, u32>>();

            //let reverse = reverse.arrange_by_key();

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .join_core(&reverse, |m2, m1, p| Some((*m2, *m1, *p)))
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
    })
    .expect("Computation terminated abnormally");
}
