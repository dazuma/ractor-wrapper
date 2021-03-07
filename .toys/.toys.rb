expand :clean, paths: :gitignore

expand :minitest, libs: ["lib", "test"], bundler: true

expand :rubocop, bundler: true

expand :yardoc do |t|
  t.generate_output_flag = true
  t.fail_on_warning = true
  t.fail_on_undocumented_objects = true
  t.bundler = true
end

expand :gem_build

expand :gem_build, name: "release", push_gem: true

expand :gem_build, name: "install", install_gem: true

tool "ci" do
  desc "Run all CI checks"

  long_desc "The 'ci' tool runs all CI checks, including unit tests," \
              " rubocop, and documentation checks. It is useful for running" \
              " tests locally during normal development, as well as being" \
              " an entrypoint for CI systems. Any failure will result in a" \
              " nonzero result code."

  include :exec, result_callback: :handle_result
  include :terminal

  def handle_result(result)
    if result.success?
      puts("** #{result.name} passed\n\n", :green, :bold)
    else
      puts("** CI terminated: #{result.name} failed!", :red, :bold)
      exit(1)
    end
  end

  def run
    exec_tool(["test"], name: "Tests")
    exec_tool(["rubocop"], name: "Style checker")
    exec_tool(["yardoc"], name: "Docs generation")
    exec_tool(["build"], name: "Gem build")
  end
end
