# Cryo **State: Not Runnable**
[![CI](https://github.com/DavisRayM/cryo/actions/workflows/ci.yml/badge.svg)](https://github.com/DavisRayM/cryo/actions/workflows/ci.yml)

Cryo is a **WIP** database system. It's composable, meaning components are built
to be plug-and-play, so new pieces can be added later with minimal friction.

This is a hobby project, and I'm learning as I go. Correctness reflects my
current understanding of each topic and will improve over time. What I
prioritize most is configurability, so I can experiment with different
mechanics, and observability, so I can see what's happening under the hood and
debug effectively. Performance and storage efficiency take a back seat to both.

## Progress

### Storage
- [X] Define a page structure on disk
- [ ] Software managed cache 🚧
   - [X] Page latching and locks
   - [X] Page cache ([Clock](https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock) algorithm)
   - [X] Controlled page access
   - [ ] Pluggable flush/allocate/mutate guards 🚧
   - [ ] Tracked page access: all writes through mutable page APIs should be
         tracked and reported to any party guarding that activity
         *(design not finalized)*
   - [ ] Pool manager should attempt to open the file handle with [O_DIRECT](https://man7.org/linux/man-pages/man2/open.2.html) 🚧

### Recovery
- [ ] ARIES [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) component 🚧
  - Allow [steal/no-force](https://kenwagatsuma.com/blog/db-steal-force-policies) policy. Configurable
- [ ] ARIES recovery

### Transaction management 🚧
- ...

### Storage server / execution engine 🚧
- [ ] Usable server/client binaries

## Resources / Good reads

- [Database Internals](https://www.databass.dev/)
- [Clarifying Direct IO's semantics (ext4 wiki)](https://archive.kernel.org/oldwiki/ext4.wiki.kernel.org/index.php/Clarifying_Direct_IO's_Semantics.html)
- [Why O_DIRECT no longer exists in libc (Rust forum)](https://users.rust-lang.org/t/why-o-direct-no-longer-exists-in-libc/121491/3)
- [O_DIRECT semantics discussion (Stack Overflow)](https://stackoverflow.com/questions/41257656/what-does-o-direct-really-mean)
- [Steal/no-force policies](https://kenwagatsuma.com/blog/db-steal-force-policies)
