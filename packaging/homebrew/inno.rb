class Inno < Formula
  desc "InnoDB file analysis toolkit for MySQL tablespace files"
  homepage "https://github.com/ringo380/idb-utils"
  url "https://github.com/ringo380/idb-utils/archive/refs/tags/v2.1.0.tar.gz"
  sha256 "PLACEHOLDER"
  license "MIT"
  head "https://github.com/ringo380/idb-utils.git", branch: "master"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args
    man1.install Dir["target/release/build/innodb-utils-*/out/man/*.1"]
    completions_dir = Dir["target/release/build/innodb-utils-*/out/completions"].first
    if completions_dir
      bash_completion.install "#{completions_dir}/inno.bash" => "inno"
      zsh_completion.install "#{completions_dir}/_inno"
      fish_completion.install "#{completions_dir}/inno.fish"
    end
  end

  test do
    assert_match "inno #{version}", shell_output("#{bin}/inno --version")
  end
end
