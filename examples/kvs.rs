use clap::Parser;

#[derive(Parser)]
struct Args {}

fn main() -> orfail::Result<()> {
    let args = Args::parse();
    Ok(())
}
