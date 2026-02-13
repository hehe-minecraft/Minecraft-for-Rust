//! Multi-thread model.

use std::sync::{Arc, Mutex};

const THREAD_DEFAULT_NAME: &str = "Thread";
const THREAD_POOL_THREAD_PREFIX: &str = "Thread #";
const THREAD_CREATE_FAILED_EXPECT: &str =
    "failed to create thread; your system may have no spare space to run this program";

/// An std thread wrapper.
pub struct Thread {
    pub name: String,
    thread: std::thread::JoinHandle<()>,
}

impl Thread {
    /// Create a new thread.
    ///
    /// **If the thread cannot be generated due to system constraints, the function will panic.**
    pub fn new<Function>(name: &str, mainloop: Function) -> Self
    where
        Function: FnOnce() + Send + 'static,
    {
        let thread = std::thread::Builder::new()
            .name(String::from(name))
            .spawn(mainloop)
            .expect(THREAD_CREATE_FAILED_EXPECT);
        Self {
            name: String::from(name),
            thread,
        }
    }

    /// Create a new thread using default name, running `mainloop`.
    ///
    /// The thread name can be toggled after creation.
    ///
    /// **If the thread cannot be generated due to system constraints, the function will panic.**
    pub fn spawn<Function>(mainloop: Function) -> Self
    where
        Function: FnOnce() + Send + 'static,
    {
        Self::new(THREAD_DEFAULT_NAME, mainloop)
    }

    /// Wait until the inner thread finishes.
    ///
    /// This function is safe to use.
    /// When the inner thread panics, the caller thread remains. For example:
    /// ```
    /// use main::thread::Thread;
    /// let thread = Thread::spawn(|| panic!("The inner thread panics!"));
    /// thread.join(); // It would be safe here.
    /// ```
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

enum Message {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Terminate,
}

/// A group of threads.
pub struct ThreadPool {
    threads: Vec<Thread>,
    sender: std::sync::mpsc::Sender<Message>,
}

impl ThreadPool {
    /// Generate a group with `size` threads, where `size` can be 0.
    ///
    /// **If the threads cannot be generated due to system constraints, the function will panic.**
    pub fn with_size(size: usize) -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut threads = Vec::with_capacity(size);
        for id in 0..size {
            let in_thread_receiver = receiver.clone();
            let name = String::from(THREAD_POOL_THREAD_PREFIX) + &id.to_string();
            let thread = Thread::new(&name, move || Self::mainloop(in_thread_receiver));
            threads.push(thread);
        }
        Self { threads, sender }
    }

    fn mainloop(receiver: Arc<Mutex<std::sync::mpsc::Receiver<Message>>>) {
        loop {
            let message = receiver.lock().unwrap().recv();
            // Since getting message is instant, the first unwrap will never panic.
            let message = match message {
                Ok(result) => result,
                Err(_) => break, // If the sender disappears, just stop.
            };
            match message {
                Message::Job(function) => function(),
                Message::Terminate => break,
            };
        }
    }

    /// Send a job (a callback function) to inner threads.
    pub fn send<Function>(&self, callback: Function)
    where
        Function: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::Job(Box::new(callback))).unwrap();
        // This will never fail, since ThreadPool always have threads in it.
    }

    /// Wait until all inner threads have stopped.
    ///
    /// Inner threads will finish **all** remaining job before they stops.
    pub fn join(self) {
        for _ in 0..self.threads.len() {
            self.sender.send(Message::Terminate).unwrap();
            // This will never fail, since ThreadPool always have threads in it.
        }
        for thread in self.threads {
            thread.join();
        }
    }
}
