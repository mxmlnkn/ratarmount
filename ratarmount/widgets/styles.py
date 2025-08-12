import logging
import re

from .resources.resources import load_resource

logger = logging.getLogger(__name__)


MODE = 'dark'

# fmt: off
COLORS = {
    # Accents
    'GREEN_PRIMARY'         : '#0C0',
    'GREEN_DARK'            : '#0A0',
    'GREEN_DARKER'          : '#080',
    'PURPLE'                : '#808',
    'PURPLE_BRIGHT'         : '#B0B',
    'RED_PRIMARY'           : '#F33',
    'RED_DARK'              : '#800',
    'ORANGE'                : '#FFA500',
    'YELLOW'                : '#FFFF00',
}

PARAMETERS = {}

PARAMETERS['dark'] = COLORS.copy()
PARAMETERS['dark'].update({
    'MODE'                  : 'dark',
    # Backgrounds
    'BASE'                  : '#000',
    'WINDOW'                : '#181818',
    'BACKGROUND_HOVER'      : '#282828',
    # Probably should match BORDER_FOCUS and BUTTON_PRESSED. However, it might be too bright, then (C0C).
    'BACKGROUND_SELECTED'   : COLORS['PURPLE_BRIGHT'],
    # Text colors
    'TEXT'                  : '#FFF',
    'TEXT_DISABLED'         : '#999999',
    'TEXT_INVERSE'          : '#000',
    'TEXT_SELECTED'         : '#FFF',
    # Borders
    'BORDER_NORMAL'         : '#333',
    # Anything but gray looks overdone for large elements such as QTreeView.
    # Smaller elements such as QLineEdit would be fine but then we are running out of coloring for hover and pressed.
    'BORDER_FOCUS'          : COLORS['PURPLE_BRIGHT'],
    'BORDER_WARNING'        : COLORS['ORANGE'],
    'BORDER_ERROR'          : COLORS['RED_PRIMARY'],
    'BORDER_RADIUS'         : '1ex',
    # Button states
    'BUTTON_NORMAL'         : '#333',
    'BUTTON_HOVER'          : '#666',
    # I feel like this should be the same as BORDER_FOCUS, or else it looks meh especially as the scroll bar
    # is also affected by this. Definitely not simply another shade, although a different color might work.
    'BUTTON_PRESSED'        : COLORS['PURPLE_BRIGHT'],
})

COLORS = {
    # Accents
    'GREEN_PRIMARY'         : '#0E0',
    'GREEN_DARK'            : '#0D0',
    'GREEN_DARKER'          : '#0C0',
    'PURPLE'                : '#A0A',
    'PURPLE_BRIGHT'         : '#D0D',
    'RED_PRIMARY'           : '#A00',
    'RED_DARK'              : '#800',
    'ORANGE'                : '#FFA500',
    'YELLOW'                : '#FFFF00',
}

PARAMETERS['light'] = COLORS.copy()
PARAMETERS['light'].update({
    'MODE'                  : 'light',
    # Backgrounds
    'BASE'                  : '#FFF',
    'WINDOW'                : '#E8E8E8',
    'BACKGROUND_HOVER'      : '#D8D8D8',
    # Probably should match BORDER_FOCUS and BUTTON_PRESSED. However, it might be too bright, then (C0C).
    'BACKGROUND_SELECTED'   : COLORS['PURPLE_BRIGHT'],
    # Text colors
    'TEXT'                  : '#000',
    'TEXT_DISABLED'         : '#999999',
    'TEXT_INVERSE'          : '#FFF',
    'TEXT_SELECTED'         : '#000',
    # Borders
    'BORDER_NORMAL'         : '#CCC',
    # Anything but gray looks overdone for large elements such as QTreeView.
    # Smaller elements such as QLineEdit would be fine but then we are running out of coloring for hover and pressed.
    'BORDER_FOCUS'          : COLORS['PURPLE_BRIGHT'],
    'BORDER_WARNING'        : COLORS['ORANGE'],
    'BORDER_ERROR'          : COLORS['RED_PRIMARY'],
    'BORDER_RADIUS'         : '1ex',
    # Button states
    'BUTTON_NORMAL'         : '#CCC',
    'BUTTON_HOVER'          : '#999',
    # I feel like this should be the same as BORDER_FOCUS, or else it looks meh especially as the scroll bar
    # is also affected by this. Definitely not simply another shade, although a different color might work.
    'BUTTON_PRESSED'        : COLORS['PURPLE_BRIGHT'],
})
# fmt: on


def get_stylesheet() -> str:
    if data := load_resource("ratarmount.css"):
        data = data.decode('utf-8')
        for key, value in PARAMETERS[MODE].items():
            data = data.replace(f"<<<{key}>>>", value)

        if placeholders := re.findall(r"<<<([^>]*)>>>", data):
            logger.warning("Unreplaced placeholders: %s", placeholders)

        return data
    return ""


def get_color(name):
    return PARAMETERS[MODE].get(name.upper(), '#FFFFFF')
