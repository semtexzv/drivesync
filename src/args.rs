
use argh::FromArgs;

#[derive(FromArgs, PartialEq, Eq, Debug)]
#[argh(subcommand, name = "up", description = "Upload files to google drive")]
pub struct Up {
    #[argh(switch, short = 'r', description = "recursively copy directories")]
    pub recursive: bool,
    #[argh(positional)]
    pub from: String,
    #[argh(positional)]
    pub to: String
}

#[derive(FromArgs, PartialEq, Eq, Debug)]
#[argh(subcommand)]
pub enum Nested {
    Up(Up)
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(name = "up", description = "Upload files to google drive")]
pub struct Args {
    #[argh(subcommand)]
    pub nested: Nested
}
