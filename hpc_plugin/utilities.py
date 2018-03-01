########
# Copyright (c) 2018 HLRS - hpcgogol@hlrs.de
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Set of useful utilities for passing files, composing Shell scripts, etc
"""

# Import proper implementation of shell lexical quoting
try:                 # python3
    from shlex import quote as shlex_quote  # noqa: F401
except ImportError:  # python2
    # from shellescape import quote as shlex_quote
    from pipes import quote as shlex_quote  # noqa: F401
