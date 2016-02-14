In order to use bash completion, you can have a `~/.bash_completion` like this:
```sh
if [ -d "$HOME/.bash_completion.d" ]; then
	for file in "$HOME/.bash_completion.d/"*
	do
		source "$file" >/dev/null 2>&1
	done
fi
```
and then copy our `contrib/bash_completion/b2` to your `~/.bash_completion.d/`.

The important trick is that `b2` tool must be in PATH before bash_completions are loaded for the last time (unless you delete the first line of our completion script).

If you keep the `b2` tool in `~/bin`, you can make sure the loading order is proper by making sure `~/bin` is added to the PATH before loading bash_completion. To do that, add the following snippet to your `~/.bashrc`:

```sh
if [ -d ~/bin ]; then
    PATH="$HOME/bin:$PATH"
fi

# enable programmable completion features (you don't need to enable
# this, if it's already enabled in /etc/bash.bashrc and /etc/profile
# sources /etc/bash.bashrc).
if ! shopt -oq posix; then
  if [ -f /usr/share/bash-completion/bash_completion ]; then
    . /usr/share/bash-completion/bash_completion
  elif [ -f /etc/bash_completion ]; then
    . /etc/bash_completion
  fi
fi
```

