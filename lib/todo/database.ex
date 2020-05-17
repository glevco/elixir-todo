defmodule Todo.Database do
  use GenServer

  @db_folder "./persist"
  @num_workers 3

  def start do
    GenServer.start(__MODULE__, nil, name: __MODULE__)
  end

  def store(key, data) do
    key
    |> choose_worker()
    |> Todo.DatabaseWorker.store(key, data)
  end

  def get(key) do
    key
    |> choose_worker()
    |> Todo.DatabaseWorker.get(key)
  end

  def init(_) do
    File.mkdir_p!(@db_folder)

    {:ok, start_workers()}
  end

  defp choose_worker(key) do
    GenServer.call(__MODULE__, {:choose_worker, key})
  end

  def handle_call({:choose_worker, key}, _, workers) do
    hash = :erlang.phash2(key, @num_workers)

    {:reply, workers[hash], workers}
  end

  defp start_workers do
    for index <- 1..@num_workers, into: %{} do
      {:ok, worker} = Todo.DatabaseWorker.start(@db_folder)

      {index - 1, worker}
    end
  end
end
