defmodule TyrexStrategyTest do
  use ExUnit.Case, async: true

  # A strategy returns a routing key, never a runtime index. `PartitionSupervisor` owns the
  # mapping from key to partition and documents that it may change, so a strategy may rely on
  # the same key reaching the same runtime and on nothing else. An integer key routes through
  # `rem(abs(key), size)`, which is why a key meant to spread must not be one.
  describe "Random strategy" do
    test "returns a different key on every call" do
      keys = for _ <- 1..100, do: Tyrex.Pool.Strategy.Random.select(:test_pool, 4, [])

      assert keys |> Enum.uniq() |> length() == 100
    end

    test "returns a non-integer key so the routing hashes it" do
      keys = for _ <- 1..100, do: Tyrex.Pool.Strategy.Random.select(:test_pool, 4, [])

      refute Enum.any?(keys, &is_integer/1)
    end
  end

  describe "Hash strategy" do
    test "returns the given key unchanged" do
      assert Tyrex.Pool.Strategy.Hash.select(:test_pool, 4, key: "user_abc") == "user_abc"
    end

    test "returns a different non-integer key when no key is given" do
      keys = for _ <- 1..100, do: Tyrex.Pool.Strategy.Hash.select(:test_pool, 4, [])

      assert keys |> Enum.uniq() |> length() == 100
      refute Enum.any?(keys, &is_integer/1)
    end
  end
end
