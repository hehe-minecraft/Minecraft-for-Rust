use main::thread::*;

#[test]
fn thread_pool_normal() {
    let (sender, receiver) = std::sync::mpsc::channel();
    let thread_pool = ThreadPool::with_size(3);
    for job_index in 0..10 {
        let sender = sender.clone();
        thread_pool.send(move || sender.send(job_index).unwrap());
    }
    drop(sender);
    thread_pool.join();
    assert_eq!(receiver.iter().count(), 10)
}

#[test]
#[ignore]
fn thread_pool_time_costing() {
    let thread_pool = ThreadPool::with_size(1);
    let start_time = std::time::Instant::now();
    thread_pool.send(|| std::thread::sleep(std::time::Duration::from_secs(1)));
    thread_pool.join();
    assert!(start_time.elapsed() > std::time::Duration::from_secs(1));
}

#[test]
#[ignore]
fn thread_pool_finish_all() {
    let thread_pool = ThreadPool::with_size(1);
    let start_time = std::time::Instant::now();
    thread_pool.send(|| std::thread::sleep(std::time::Duration::from_secs(1)));
    thread_pool.send(|| std::thread::sleep(std::time::Duration::from_secs(1)));
    thread_pool.join();
    assert!(start_time.elapsed() > std::time::Duration::from_secs(1 + 1));
}

#[test]
fn thread_pool_panic() {
    let thread_pool = ThreadPool::with_size(3);
    thread_pool.send(|| panic!("One of the thread panics"));
    thread_pool.join(); // It should not panic.
}

#[test]
fn thread_normal() {
    let (sender, receiver) = std::sync::mpsc::channel();
    let thread = Thread::spawn(move || sender.send(true).unwrap());
    thread.join();
    assert_eq!(receiver.try_recv().unwrap(), true);
}

#[test]
fn thread_join_panic() {
    let thread = Thread::spawn(|| panic!("Inner thread panics"));
    thread.join(); // It should not panic.
}
