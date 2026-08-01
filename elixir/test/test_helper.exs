# `function_exported?/3` reports FALSE for a module that has not been loaded
# yet, and under `mix test` modules load lazily. Most of the model suites assert
# their module's exports that way, so they were failing against functions that
# exist — failures that read "not implemented" and meant "not loaded".
#
# Loading them up front makes those assertions measure what they claim to.
for module <- [
      Nucleus.Client,
      Nucleus.Repo,
      Nucleus.Retry,
      Nucleus.Migration,
      Nucleus.Models.SQL,
      Nucleus.Models.KV,
      Nucleus.Models.Vector,
      Nucleus.Models.TimeSeries,
      Nucleus.Models.Document,
      Nucleus.Models.Graph,
      Nucleus.Models.FTS,
      Nucleus.Models.Geo,
      Nucleus.Models.Blob,
      Nucleus.Models.Streams,
      Nucleus.Models.Columnar,
      Nucleus.Models.Datalog,
      Nucleus.Models.CDC,
      Nucleus.Models.PubSub
    ] do
  Code.ensure_loaded(module)
end

ExUnit.start()
