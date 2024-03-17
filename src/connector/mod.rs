pub mod data_gen;
mod print;
mod file_system;
mod black_hole;
mod value;

pub use print::Print;
pub use black_hole::BlackHole;
pub use file_system::FileSystemConnector;
pub use value::ValueConnector;

use futures_async_stream::try_stream;
use crate::array::DataChunk;
use crate::stream::{Barrier, Message};
use crate::executor::ExecuteError;

pub trait StreamConnector {


    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn read(&self);


    fn write(&mut self, chunk: DataChunk);

    fn on_receive_barrier(&mut self, barrier: Barrier);
}