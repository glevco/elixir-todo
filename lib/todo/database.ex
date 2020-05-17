defmodule Todo.Database do
  use GenServer

  @db_folder "./persist"
  @num_workers 3

  def start do
    GenServer.start(__MODULE__, nil, name: __MODULE__)
  end

  def store(key, data) do
    GenServer.cast(__MODULE__, {:store, key, data})
  end

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  def init(_) do
    File.mkdir_p!(@db_folder)
    worker_nums = 0..(@num_workers - 1)
    worker_generator = &{&1, get_worker()}
    workers = Map.new(worker_nums, worker_generator)

    {:ok, workers}
  end

  def handle_cast({:store, key, data}, workers) do
    workers
    |> choose_worker(key)
    |> Todo.DatabaseWorker.store(key, data)

    {:noreply, workers}
  end

  def handle_call({:get, key}, _, workers) do
    data =
      workers
      |> choose_worker(key)
      |> Todo.DatabaseWorker.get(key)

    {:reply, data, workers}
  end

  defp get_worker do
    {_, worker} = Todo.DatabaseWorker.start(@db_folder)

    worker
  end

  def choose_worker(workers, key) do
    hash = :erlang.phash2(key, @num_workers)
    workers[hash]
  end
end
